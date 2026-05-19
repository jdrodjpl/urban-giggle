"""Pluggable input-source getters for the Frozon ingest pipelines.

Each source resolves a set of input TIFFs that the downstream COG/Zarr workers
can ingest. A source is selected at runtime via `--input-source-type`. The
common contract is `InputSource.list_inputs() -> List[InputRef]`.
"""

from __future__ import annotations

import argparse
import logging
from typing import Optional

from .base import InputRef, InputSource
from .s3_prefix import S3PrefixSource

logger = logging.getLogger(__name__)

__all__ = ["InputRef", "InputSource", "S3PrefixSource", "make_source", "ensure_edl_login"]


def make_source(args: argparse.Namespace) -> InputSource:
    """Construct an InputSource from CLI args based on `--input-source-type`.

    Recognized types:
        s3  — `--input-s3-prefix` listing.
        cmr — CMR collection search via earthaccess.

    Anything else raises ValueError.
    """
    source_type = (getattr(args, 'input_source_type', None) or 's3').lower()

    if source_type == 's3':
        prefix = getattr(args, 'input_s3_prefix', None)
        if not prefix:
            raise ValueError("--input-source-type=s3 requires --input-s3-prefix")
        return S3PrefixSource(
            s3_prefix=prefix,
            role_arn=getattr(args, 'role_arn', None),
            filter_pattern=getattr(args, 'filter_pattern', None),
            limit=getattr(args, 'limit', None),
        )

    if source_type == 'cmr':
        from .cmr_tiff import CMRTiffSource
        short_name = getattr(args, 'cmr_short_name', None)
        if not short_name:
            raise ValueError("--input-source-type=cmr requires --cmr-short-name")

        temporal = None
        start = getattr(args, 'cmr_temporal_start', None)
        end = getattr(args, 'cmr_temporal_end', None)
        if start and end:
            temporal = (start, end)
        elif start or end:
            raise ValueError("--cmr-temporal-start and --cmr-temporal-end must be set together")

        bbox = None
        bbox_arg = getattr(args, 'cmr_bbox', None)
        if bbox_arg:
            try:
                bbox = tuple(float(x) for x in bbox_arg.split(','))
                if len(bbox) != 4:
                    raise ValueError
            except ValueError:
                raise ValueError(
                    "--cmr-bbox must be 'west,south,east,north' floats, got "
                    f"{bbox_arg!r}"
                )

        granule_ids = getattr(args, 'cmr_granule_ids', None)
        if granule_ids:
            granule_ids = [g.strip() for g in granule_ids.split(',') if g.strip()]

        return CMRTiffSource(
            short_name=short_name,
            version=getattr(args, 'cmr_version', None),
            temporal=temporal,
            bbox=bbox,
            granule_ids=granule_ids,
            prefer_https=getattr(args, 'cmr_prefer_https', True),
            limit=getattr(args, 'limit', None),
        )

    raise ValueError(f"Unsupported --input-source-type: {source_type!r}")


def ensure_edl_login(secret_name: Optional[str], maap_instance=None) -> bool:
    """Log into Earthdata if a secret name is given. Returns True if a login
    was performed. Safe to call multiple times (earthaccess caches the session).
    """
    if not secret_name:
        return False
    from .cmr_tiff import login_from_maap_secret
    login_from_maap_secret(secret_name, maap_instance=maap_instance)
    return True
