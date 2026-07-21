"""Copernicus Data Space Ecosystem (CDSE) input source for Sentinel-1 SAFE ZIPs.

CDSE is the origin catalog for Sentinel data — ASF mirrors it with a lag
that has been observed to reach ~10 days. This module lets the S1 GRD
pipeline fall back to CDSE for dates ASF hasn't mirrored yet:

- `search_products` — unauthenticated OData catalog query. Used both by
  the GH Actions cron (per-date counts, to decide ASF vs CDSE per date)
  and by the worker (to resolve the products it will download). Keeping
  one implementation guarantees the cron's count and the worker's
  download list apply identical filters.
- `resolve_cdse_creds` / `CDSEAuth` — OAuth2 token from a MAAP secret.
- `download_product` — authenticated product download that survives the
  cross-host redirect CDSE's download endpoint issues (requests drops
  the Authorization header on cross-host redirects, so we follow them
  manually).

CDSE catalogs every GRD acquisition twice — once as the classic SAFE
(`...XXXX.SAFE`) and once in COG format (`...XXXX_COG.SAFE`). The COG
variants are excluded here: the worker's calibration path reads DN +
LUTs out of the classic SAFE ZIP, and ASF serves the classic layout,
so excluding COG keeps the two sources byte-compatible.

Module-level imports are stdlib + requests only, and there are no
relative imports — the GH Actions cron loads this file standalone
(sys.path onto src/input_sources/) without pulling in the rest of the
package's boto3/maap dependency chain.
"""

from __future__ import annotations

import logging
import os
import re
import time
from fnmatch import fnmatch
from pathlib import Path
from typing import List, Optional, Sequence

import requests

logger = logging.getLogger(__name__)

ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
TOKEN_URL = ("https://identity.dataspace.copernicus.eu/auth/realms/CDSE"
             "/protocol/openid-connect/token")
DOWNLOAD_URL_TEMPLATE = ("https://download.dataspace.copernicus.eu/odata/v1"
                         "/Products({pid})/$value")
# CDSE's public OAuth client for username/password (resource-owner) grants.
PUBLIC_CLIENT_ID = "cdse-public"
PAGE_SIZE = 1000
MAX_REDIRECTS = 5

# CMR-short-name suffix → the product-type token embedded in CDSE Names
# (S1A_EW_GRDM_1SDH_...): GRD_MEDIUM → GRDM etc.
_PRODUCT_TYPE_TOKENS = {
    "GRD_MEDIUM": "_GRDM_",
    "GRD_HIGH":   "_GRDH_",
    "GRD_FULL":   "_GRDF_",
}


# --------------------------------------------------------------------------
# CMR short-name → CDSE query mapping
# --------------------------------------------------------------------------

def platform_and_type(short_name: str) -> tuple:
    """Map a CMR short_name like SENTINEL-1A_DP_GRD_MEDIUM to the
    (platform prefix, product-type token) pair used in CDSE Names,
    e.g. ("S1A", "_GRDM_")."""
    m = re.match(r"SENTINEL-1([A-Z])", short_name.upper())
    if not m:
        raise ValueError(
            f"Can't derive a Sentinel-1 platform from CMR short_name "
            f"{short_name!r} (expected SENTINEL-1<letter>_...)."
        )
    platform = f"S1{m.group(1)}"
    token = ""
    for suffix, tok in _PRODUCT_TYPE_TOKENS.items():
        if short_name.upper().endswith(suffix):
            token = tok
            break
    if not token:
        raise ValueError(
            f"Can't derive a GRD product-type token from CMR short_name "
            f"{short_name!r} (expected a *_GRD_MEDIUM/HIGH/FULL suffix)."
        )
    return platform, token


# --------------------------------------------------------------------------
# Catalog search (no auth required)
# --------------------------------------------------------------------------

def _iso_start(value: str) -> str:
    return f"{value}T00:00:00.000Z" if len(value) == 10 else value


def _footprint_lat_range(geo_footprint) -> Optional[tuple]:
    """Extract (min_lat, max_lat) from a GeoJSON GeoFootprint of any
    nesting depth. Returns None if no coordinates are found."""
    lats: List[float] = []

    def walk(node):
        if (isinstance(node, (list, tuple)) and len(node) >= 2
                and all(isinstance(v, (int, float)) for v in node[:2])):
            lats.append(float(node[1]))
            return
        if isinstance(node, (list, tuple)):
            for child in node:
                walk(child)

    if isinstance(geo_footprint, dict):
        walk(geo_footprint.get("coordinates", []))
    if not lats:
        return None
    return min(lats), max(lats)


def search_products(
    short_names: Sequence[str],
    temporal_start: str,
    temporal_end: str,
    bbox: Optional[Sequence[float]] = None,
    filter_pattern: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> List[dict]:
    """Query the CDSE OData catalog for classic-SAFE GRD products.

    `temporal_start`/`temporal_end` are ISO dates or datetimes; the range
    is [start, end) on ContentDate/Start, matching the cron's CMR walk.
    `filter_pattern` is the same granule-name glob the CMR path uses
    (e.g. `*_1SDH_*`).

    The spatial filter is latitude-band only: a product is kept when its
    footprint's latitude range overlaps [bbox_south, bbox_north].
    Longitude is ignored — the pipeline's bbox is the full-longitude
    Arctic band, and OData geography polygons can't express an
    antimeridian-spanning band anyway. The latitude test is what
    separates Arctic from Antarctic EW acquisitions, which is the only
    spatial distinction this product family needs.

    Returns [{"id", "name", "content_start"}], deduped by product id,
    sorted by name. COG-format duplicates are always excluded.
    """
    sess = session or requests.Session()
    start, end = _iso_start(temporal_start), _iso_start(temporal_end)
    lat_band = (float(bbox[1]), float(bbox[3])) if bbox else None

    by_id: dict = {}
    for short_name in short_names:
        platform, type_token = platform_and_type(short_name)
        odata_filter = (
            f"startswith(Name,'{platform}') "
            f"and contains(Name,'{type_token}') "
            f"and ContentDate/Start ge {start} "
            f"and ContentDate/Start lt {end}"
        )
        url: Optional[str] = ODATA_URL
        params: Optional[dict] = {
            "$filter": odata_filter,
            "$select": "Id,Name,ContentDate,GeoFootprint",
            "$top": PAGE_SIZE,
        }
        while url:
            resp = sess.get(url, params=params, timeout=120)
            resp.raise_for_status()
            payload = resp.json()
            for product in payload.get("value", []):
                name = product.get("Name", "")
                if "_COG" in name:
                    continue
                if filter_pattern and not fnmatch(name, filter_pattern):
                    continue
                if lat_band:
                    lat_range = _footprint_lat_range(product.get("GeoFootprint"))
                    # No parseable footprint → keep; a false positive
                    # costs one wasted download, a false negative loses
                    # real coverage.
                    if lat_range and (lat_range[1] < lat_band[0]
                                      or lat_range[0] > lat_band[1]):
                        continue
                by_id[product["Id"]] = {
                    "id": product["Id"],
                    "name": name,
                    "content_start": (product.get("ContentDate") or {}).get("Start"),
                }
            # nextLink already encodes the query params.
            url = payload.get("@odata.nextLink")
            params = None

    products = sorted(by_id.values(), key=lambda p: p["name"])
    logger.info(
        f"CDSE returned {len(products)} matching product(s) for "
        f"{list(short_names)} in [{start}, {end})"
    )
    return products


# --------------------------------------------------------------------------
# OAuth (MAAP secret → bearer token)
# --------------------------------------------------------------------------

def resolve_cdse_creds(secret_name: str, maap_instance=None) -> dict:
    """Pull CDSE OAuth credentials from a MAAP secret.

    Accepted formats (mirroring the EDL secret conventions):
    - two lines: `username\\npassword` — resource-owner password grant
      against the public `cdse-public` client.
    - `key=value` lines with `client_id=` + `client_secret=` — client
      credentials grant for a registered private client. May also spell
      out `username=` / `password=` explicitly.
    """
    if maap_instance is None:
        from common_utils import MaapUtils
        maap_instance = MaapUtils.get_maap_instance()
    secret = maap_instance.secrets.get_secret(secret_name)
    body = secret if isinstance(secret, str) else secret.get("value", "")
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]

    kv = {}
    for ln in lines:
        if "=" in ln:
            key, _, value = ln.partition("=")
            kv[key.strip().lower()] = value.strip()

    if "client_id" in kv and "client_secret" in kv:
        return {"client_id": kv["client_id"], "client_secret": kv["client_secret"]}
    if "username" in kv and "password" in kv:
        return {"username": kv["username"], "password": kv["password"]}
    if len(lines) >= 2 and not kv:
        return {"username": lines[0], "password": lines[1]}
    raise RuntimeError(
        f"CDSE secret {secret_name!r} format not recognized — expected "
        f"'username\\npassword' lines or client_id=/client_secret= pairs."
    )


class CDSEAuth:
    """Caches a CDSE bearer token and re-grants when it nears expiry.

    CDSE access tokens live ~10 minutes while a day's downloads run for
    hours, so every `bearer()` call checks remaining lifetime. Re-running
    the original grant is deliberately preferred over refresh tokens —
    the refresh token itself expires mid-run, the grant is one request,
    and both credential shapes handle it identically.
    """

    _EXPIRY_MARGIN_S = 60

    def __init__(self, creds: dict):
        self._creds = creds
        self._token: Optional[str] = None
        self._expires_at = 0.0

    def bearer(self) -> str:
        if self._token is None or time.monotonic() >= self._expires_at:
            self._grant()
        return self._token

    def invalidate(self) -> None:
        self._token = None

    def _grant(self) -> None:
        if "client_secret" in self._creds:
            data = {
                "grant_type": "client_credentials",
                "client_id": self._creds["client_id"],
                "client_secret": self._creds["client_secret"],
            }
        else:
            data = {
                "grant_type": "password",
                "client_id": PUBLIC_CLIENT_ID,
                "username": self._creds["username"],
                "password": self._creds["password"],
            }
        resp = requests.post(TOKEN_URL, data=data, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(
                f"CDSE token grant failed ({resp.status_code}): {resp.text[:300]}"
            )
        payload = resp.json()
        self._token = payload["access_token"]
        self._expires_at = (time.monotonic()
                            + int(payload.get("expires_in", 600))
                            - self._EXPIRY_MARGIN_S)
        logger.info("CDSE OAuth token acquired.")


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------

def _get_following_redirects(url: str, token: str,
                             session: requests.Session) -> requests.Response:
    """GET with manual redirect handling so the Authorization header
    survives the hop to CDSE's download node (requests strips it on
    cross-host redirects)."""
    for _ in range(MAX_REDIRECTS):
        resp = session.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            stream=True,
            allow_redirects=False,
            timeout=300,
        )
        if resp.status_code in (301, 302, 303, 307, 308):
            url = resp.headers["Location"]
            resp.close()
            continue
        return resp
    raise RuntimeError(f"Exceeded {MAX_REDIRECTS} redirects downloading {url}")


def download_product(product: dict, dest_dir, auth: CDSEAuth,
                     session: Optional[requests.Session] = None) -> Path:
    """Download one product ({"id", "name"}) as `<Name minus .SAFE>.zip`.

    The $value payload is a ZIP of the SAFE directory — the same layout
    ASF distributes, so downstream calibration code is source-agnostic.
    Retries once with a fresh token on 401/403 (the token can expire
    between the check in `bearer()` and the server handling the request).
    """
    sess = session or requests.Session()
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    name = product["name"]
    zip_name = (name[:-5] if name.endswith(".SAFE") else name) + ".zip"
    local_path = dest_dir / zip_name
    url = DOWNLOAD_URL_TEMPLATE.format(pid=product["id"])

    for attempt in (1, 2):
        resp = _get_following_redirects(url, auth.bearer(), sess)
        if resp.status_code in (401, 403) and attempt == 1:
            logger.warning(f"CDSE download got {resp.status_code}; "
                           f"re-granting token and retrying once.")
            resp.close()
            auth.invalidate()
            continue
        if resp.status_code != 200:
            body = resp.text[:300]
            resp.close()
            raise RuntimeError(
                f"CDSE download failed for {name} ({resp.status_code}): {body}"
            )
        with resp:
            with open(local_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        fh.write(chunk)
        break

    if not local_path.exists() or local_path.stat().st_size == 0:
        raise RuntimeError(f"CDSE download produced an empty file: {local_path}")
    return local_path
