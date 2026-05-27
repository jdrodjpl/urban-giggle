"""SWOT L2 LR SSH NetCDF → GeoTIFF converters.

Stub implementations — raise NotImplementedError until the actual
conversion logic is provided. The interface and expected behavior are
documented here so the implementation can be dropped in without
touching the rest of the pipeline.

SWOT L2 LR SSH products are NetCDF4 files containing swath-based sea
surface height variables on an irregular grid. Conversion to GeoTIFF
requires:
  1. Reading one or more variables from the NetCDF
  2. Optionally reprojecting from swath coordinates to a regular grid
  3. Writing each variable as a single-band GeoTIFF with proper CRS,
     transform, and nodata handling
  4. Embedding the acquisition datetime in the filename so the
     downstream time_regex can extract it

CMR collection short_names (for reference when wiring into the CMR
input source):
  - Expert:      SWOT_L2_LR_SSH_Expert_2.0
  - Unsmoothed:  SWOT_L2_LR_SSH_Unsmoothed_2.0

Typical variables of interest:
  Expert:
    - ssha_karin             (sea surface height anomaly)
    - ssha_karin_2           (alternative estimator)
    - mss                    (mean sea surface)
    - sig0_karin             (radar backscatter — sigma0)
    - sig0_karin_2
  Unsmoothed:
    - ssha_karin
    - sig0_karin

Expected output filename pattern (must match time_regex used by the
downstream Zarr/COG pipeline):
    SWOT_L2_LR_SSH_Expert_<orbit>_<cycle>_<YYYYMMDD>T<HHMMSS>Z_<variable>.tif

    The `_YYYYMMDDTHHMMSSZ_` portion is what the standard time_regex
    `_(?P<start_date>\\d{8}T\\d{6})Z_` captures.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

from . import ConvertResult, Converter

logger = logging.getLogger(__name__)

# Default variables to extract when the caller doesn't specify.
EXPERT_DEFAULT_VARS = ["ssha_karin", "sig0_karin"]
UNSMOOTHED_DEFAULT_VARS = ["ssha_karin", "sig0_karin"]


class SWOTExpertConverter:
    """Convert a SWOT L2 LR SSH Expert NetCDF to GeoTIFF(s).

    Placeholder — implement `convert()` when the conversion logic is
    available. The interface is stable; only the body of `convert`
    needs to be filled in.
    """

    @property
    def dataset_name(self) -> str:
        return "SWOT L2 LR SSH Expert"

    def convert(
        self,
        source_path: Path,
        output_dir: Path,
        variables: Optional[List[str]] = None,
    ) -> List[ConvertResult]:
        """Convert a single SWOT Expert NetCDF to GeoTIFF(s).

        Parameters
        ----------
        source_path : Path
            Path to the downloaded .nc file.
        output_dir : Path
            Directory to write output TIFFs.
        variables : list of str, optional
            Variable names to extract. Defaults to ssha_karin + sig0_karin.

        Returns
        -------
        List[ConvertResult]
            One entry per output TIFF.

        Implementation notes
        --------------------
        When implementing, the function should:
        1. Open `source_path` with xarray or netCDF4.
        2. For each variable, read the data array + lat/lon coords.
        3. Reproject / grid as needed (swath → regular grid).
        4. Write to `output_dir/<stem>_<YYYYMMDD>T<HHMMSS>Z_<variable>.tif`
           using rasterio, with proper CRS + nodata.
        5. Return a ConvertResult per file.
        """
        variables = variables or EXPERT_DEFAULT_VARS
        raise NotImplementedError(
            f"SWOTExpertConverter.convert() is a stub. "
            f"Implement NetCDF→TIFF conversion for variables {variables} "
            f"from {source_path.name}. See docstring for expected interface."
        )


class SWOTUnsmoothedConverter:
    """Convert a SWOT L2 LR SSH Unsmoothed NetCDF to GeoTIFF(s).

    Same interface as SWOTExpertConverter — only the default variables
    and possibly the internal grid handling differ.
    """

    @property
    def dataset_name(self) -> str:
        return "SWOT L2 LR SSH Unsmoothed"

    def convert(
        self,
        source_path: Path,
        output_dir: Path,
        variables: Optional[List[str]] = None,
    ) -> List[ConvertResult]:
        """See SWOTExpertConverter.convert() for interface docs."""
        variables = variables or UNSMOOTHED_DEFAULT_VARS
        raise NotImplementedError(
            f"SWOTUnsmoothedConverter.convert() is a stub. "
            f"Implement NetCDF→TIFF conversion for variables {variables} "
            f"from {source_path.name}. See SWOTExpertConverter docstring."
        )
