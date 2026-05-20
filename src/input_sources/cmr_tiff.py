"""NASA CMR-backed input source for TIFF-native collections.

Uses the `earthaccess` library to query CMR for granules matching a
short_name/version/temporal/bbox, then exposes the resulting granule
URLs as `InputRef` records. Authentication happens via NASA Earthdata
Login — the EDL bearer token is pulled from a MAAP secret at construction
time.

This source intentionally does NOT download granules. The orchestrator
gets the list of URLs back and dispatches per-granule worker jobs that
handle the actual fetch. That keeps the orchestrator memory/disk profile
unchanged regardless of granule size.

Note: this currently targets TIFF-native CMR collections (e.g. Sentinel-1
GRD that lands as direct .tif granules). Collections that wrap TIFFs in
ZIP bundles (standard Sentinel-1 GRD/SLC from ASF) need an unwrap step
before COG conversion — handled in the worker, not here.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import List, Optional, Sequence

from .base import InputRef, InputSource

logger = logging.getLogger(__name__)


@dataclass
class CMRTiffSource:
    """Resolve TIFF inputs from a CMR collection.

    Parameters
    ----------
    short_name:
        CMR `short_name` of the target collection (e.g. "SENTINEL-1A_GRD_HD").
    version:
        Collection version. Some CMR collections require it; `None` falls
        back to whatever earthaccess decides.
    temporal:
        Tuple of (start, end) ISO date strings, inclusive. e.g.
        ("2024-01-01", "2024-01-31").
    bbox:
        Bounding box (west, south, east, north), all floats.
    granule_ids:
        Specific granule UR identifiers (overrides temporal/bbox if set).
    prefer_https:
        When True, return https:// granule URLs (require EDL token to
        download). When False, prefer s3:// URLs if CMR exposes them
        (in-region MAAP workers can read these directly with role_arn,
        no EDL needed). Defaults to True since cross-account S3 access
        isn't universally granted.
    """

    short_name: str
    version: Optional[str] = None
    temporal: Optional[tuple] = None
    bbox: Optional[tuple] = None
    granule_ids: Optional[Sequence[str]] = None
    prefer_https: bool = True
    limit: Optional[int] = None

    def __post_init__(self):
        try:
            import earthaccess  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "CMRTiffSource requires the `earthaccess` package. "
                "Add it to your conda env or `pip install earthaccess`."
            ) from e

    @property
    def description(self) -> str:
        bits = [f"short_name={self.short_name}"]
        if self.version:
            bits.append(f"version={self.version}")
        if self.temporal:
            bits.append(f"temporal={self.temporal[0]}..{self.temporal[1]}")
        if self.bbox:
            bits.append(f"bbox={self.bbox}")
        if self.granule_ids:
            bits.append(f"granule_ids=<{len(self.granule_ids)} ids>")
        return f"CMR({', '.join(bits)})"

    def list_inputs(self) -> List[InputRef]:
        """Run the CMR search and return one InputRef per granule TIFF."""
        import earthaccess

        # Caller is expected to have already authenticated via
        # `earthaccess.login(strategy="environment")` or similar. See
        # `login_from_maap_secret` below for the MAAP integration.
        results = self._search()
        logger.info(f"CMR search returned {len(results)} granule(s) for {self.description}")

        refs: List[InputRef] = []
        for granule in results:
            urls = self._pick_urls(granule)
            for url in urls:
                if not url.lower().endswith((".tif", ".tiff")):
                    # Skip companions (XML, browse images, manifest, etc.).
                    continue
                name = os.path.basename(url.split("?", 1)[0])
                refs.append(InputRef(
                    url=url,
                    name=name,
                    auth_kind="https_edl" if url.startswith("https://") else "s3",
                    metadata={
                        "concept_id": granule.get("meta", {}).get("concept-id"),
                        "granule_ur": granule.get("umm", {}).get("GranuleUR"),
                    },
                ))

        refs.sort(key=lambda r: r.name)
        if self.limit:
            refs = refs[: self.limit]
        logger.info(f"Resolved {len(refs)} TIFF input(s) from {self.description}")
        return refs

    def _search(self):
        import earthaccess

        kwargs = {"short_name": self.short_name}
        if self.version:
            kwargs["version"] = self.version
        if self.temporal:
            kwargs["temporal"] = self.temporal
        if self.bbox:
            kwargs["bounding_box"] = self.bbox
        if self.granule_ids:
            # earthaccess passes through to CMR `concept_id` / `readable_granule_name`
            kwargs["granule_name"] = list(self.granule_ids)
        return earthaccess.search_data(**kwargs)

    def _pick_urls(self, granule) -> List[str]:
        """Return preferred URLs for a granule. earthaccess granule objects
        expose `.data_links()` that respects in-region/out-of-region preference."""
        try:
            if self.prefer_https:
                return granule.data_links(access="external")
            return granule.data_links(access="direct")
        except Exception:
            # Older earthaccess versions: fall back to manual extraction.
            links = granule.get("umm", {}).get("RelatedUrls", [])
            urls = [
                l["URL"] for l in links
                if l.get("Type") == "GET DATA"
            ]
            return urls


def login_from_maap_secret(secret_name: str, maap_instance=None) -> None:
    """Resolve an EDL bearer token from a MAAP secret and log into earthaccess.

    The MAAP secret should contain either:
    - An EDL bearer token (`token=...`), OR
    - A `username\\npassword` pair for username/password auth.

    Sets the `EARTHDATA_USERNAME`/`EARTHDATA_PASSWORD` env vars or the
    `EARTHDATA_TOKEN` env var (depending on what's in the secret) and then
    runs `earthaccess.login(strategy="environment")`.
    """
    import earthaccess
    from common_utils import MaapUtils

    maap = maap_instance or MaapUtils.get_maap_instance()
    secret = maap.secrets.get_secret(secret_name)
    body = secret if isinstance(secret, str) else secret.get("value", "")

    if body.startswith("token=") or len(body.strip().splitlines()) == 1 and "=" not in body:
        token = body.split("=", 1)[-1].strip()
        os.environ["EARTHDATA_TOKEN"] = token
    else:
        lines = [ln for ln in body.splitlines() if ln.strip()]
        if len(lines) >= 2:
            os.environ["EARTHDATA_USERNAME"] = lines[0].strip()
            os.environ["EARTHDATA_PASSWORD"] = lines[1].strip()

    auth = earthaccess.login(strategy="environment")
    if not auth or not auth.authenticated:
        raise RuntimeError(
            f"EDL auth failed using MAAP secret '{secret_name}'. "
            "Confirm the secret contains a valid token or username/password."
        )
    logger.info("Earthdata Login successful via MAAP secret.")
