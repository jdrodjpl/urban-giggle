"""S3-prefix input source — wraps the original orchestrator listing logic
so callers can use a uniform InputSource interface regardless of whether
inputs come from S3 or CMR."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import List, Optional

from common_utils import AWSUtils

from .base import InputRef, InputSource

logger = logging.getLogger(__name__)


@dataclass
class S3PrefixSource:
    """List TIFFs under an S3 prefix."""

    s3_prefix: str
    role_arn: Optional[str] = None
    filter_pattern: Optional[str] = None
    limit: Optional[int] = None

    @property
    def description(self) -> str:
        bits = [f"s3_prefix={self.s3_prefix}"]
        if self.filter_pattern:
            bits.append(f"filter={self.filter_pattern}")
        return f"S3({', '.join(bits)})"

    def list_inputs(self) -> List[InputRef]:
        bucket, prefix = AWSUtils.parse_s3_path(self.s3_prefix.rstrip('/'))
        s3 = AWSUtils.get_s3_client(role_arn=self.role_arn, bucket_name=bucket)
        paginator = s3.get_paginator('list_objects_v2')

        refs: List[InputRef] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if not key.lower().endswith(('.tif', '.tiff')):
                    continue
                if self.filter_pattern and not fnmatch(os.path.basename(key), self.filter_pattern):
                    continue
                refs.append(InputRef(
                    url=f"s3://{bucket}/{key}",
                    name=os.path.basename(key),
                    auth_kind="s3",
                ))

        refs.sort(key=lambda r: r.name)
        if self.limit:
            refs = refs[: self.limit]
        logger.info(f"Discovered {len(refs)} TIFF input(s) under {self.s3_prefix}")
        return refs
