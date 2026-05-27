"""Converter modules for non-TIFF source datasets.

Each converter takes a single source file (NetCDF, HDF5, etc.) and
produces one or more GeoTIFFs in an output directory. The resulting
TIFFs feed into the existing COG/Zarr pipelines unchanged.

Usage:
    from converters import get_converter
    converter = get_converter("swot-ssh-expert")
    tiffs = converter.convert(nc_path, output_dir)

When a dataset's converter is not yet implemented, `convert()` raises
NotImplementedError with a message describing what's needed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Protocol


@dataclass(frozen=True)
class ConvertResult:
    """One output TIFF from a conversion."""
    path: Path
    variable: str
    datetime_str: Optional[str] = None
    metadata: dict = field(default_factory=dict)


class Converter(Protocol):
    """Convert a single source file to one or more GeoTIFFs."""

    @property
    def dataset_name(self) -> str:
        """Human-readable dataset identifier for log messages."""
        ...

    def convert(self, source_path: Path, output_dir: Path,
                variables: Optional[List[str]] = None) -> List[ConvertResult]:
        """Convert `source_path` to GeoTIFF(s) in `output_dir`.

        Parameters
        ----------
        source_path:
            Path to the downloaded source file (.nc, .h5, etc.)
        output_dir:
            Directory to write output TIFFs. Created if it doesn't exist.
        variables:
            Optional list of variable names to extract. If None, the
            converter's default set is used.

        Returns
        -------
        List of ConvertResult, one per output TIFF.
        """
        ...


def get_converter(dataset_key: str) -> Converter:
    """Look up a converter by dataset key.

    Known keys:
        swot-ssh-expert       — SWOT L2 LR SSH Expert (PO.DAAC, NetCDF)
        swot-ssh-unsmoothed   — SWOT L2 LR SSH Unsmoothed (PO.DAAC, NetCDF)
        icesat2-atl10ql       — ICESat-2 ATL10QL v7 freeboard (NSIDC, HDF5)

    Raises KeyError for unknown dataset keys.
    """
    from .swot_nc import SWOTExpertConverter, SWOTUnsmoothedConverter

    _registry = {
        "swot-ssh-expert": SWOTExpertConverter,
        "swot-ssh-unsmoothed": SWOTUnsmoothedConverter,
        # "icesat2-atl10ql": ICESat2ATL10Converter,  # TODO
    }

    cls = _registry.get(dataset_key)
    if cls is None:
        raise KeyError(
            f"No converter registered for {dataset_key!r}. "
            f"Known keys: {sorted(_registry.keys())}"
        )
    return cls()
