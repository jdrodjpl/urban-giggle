"""Protocol shared by all input-source getters."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Protocol


@dataclass(frozen=True)
class InputRef:
    """One resolved input TIFF, ready for a worker job to fetch and ingest.

    `url` may be s3:// (read with `role_arn`) or https:// (read with an EDL
    bearer token — see CMRSource). Callers are responsible for honoring the
    scheme when staging.
    """
    url: str
    name: str
    auth_kind: str = "s3"
    metadata: dict = field(default_factory=dict)


class InputSource(Protocol):
    """Resolve a set of input TIFFs.

    Implementations should be cheap to construct from CLI args (or a config
    dict) and defer network calls to `list_inputs()` so that orchestrator
    dry-run/validation paths can avoid them.
    """

    def list_inputs(self) -> List[InputRef]:
        ...

    @property
    def description(self) -> str:
        """Short human-readable description used in log lines."""
        ...
