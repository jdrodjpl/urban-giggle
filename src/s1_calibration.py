"""Sentinel-1 GRD radiometric calibration → σ⁰ / β⁰ / γ⁰ in **decibels**.

GDAL's SAFE driver exposes the raw DN ("UNCALIB") subdatasets but
doesn't apply σ⁰ / β⁰ / γ⁰ calibration for GRD products in any current
GDAL version. This module applies them ourselves from the per-pixel
calibration LUT in the SAFE annotation XML:

    calibrated_linear(i,j) = DN(i,j)² / k(i,j)²
    calibrated_db(i,j)     = 10 * log10(calibrated_linear)

where k(i,j) is the relevant calibration value (`sigmaNought`,
`betaNought`, `gamma`, …) interpolated bilinearly from the
calibration vector grid. The formula is identical across conventions;
only the LUT differs. See ESA's Sentinel-1 Product Specification
Section 9.2.

The output is **σ⁰ in decibels** (Float32). Decibels are how SAR
imagery is conventionally displayed and reported, and storing in dB
matches NSIDC / Polar View / standard sea-ice products plus the
sibling `s1_calibrate.py` Earth Engine workflow used elsewhere in the
project. The downstream MMGIS / TiTiler config can rescale directly
(e.g. -30 to +10 dB) without a layer-side
`expression=10*log10(b1)` transformation.

Each calibration writes a separate Float32 GeoTIFF; NaN for pixels
where DN was 0 (no signal / off-swath). The output preserves the
SAFE driver's GCPs so a downstream gdalwarp can geocode it.

References:
- ESA Sentinel-1 Product Specification, Section 9.2
- https://sentinels.copernicus.eu/web/sentinel/radiometric-calibration-of-level-1-products
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Tuple
from xml.etree import ElementTree as ET

import numpy as np
import rasterio
from scipy.interpolate import RegularGridInterpolator


def find_calibration_xml(zip_path: Path, polarization: str) -> str:
    """Return the SAFE-internal path to the calibration XML for the
    requested polarization (e.g. "hh"). Looks for the file matching
    `calibration/calibration-*-<pol>-*.xml`.
    """
    with zipfile.ZipFile(str(zip_path)) as zf:
        for name in zf.namelist():
            if (
                "annotation/calibration/calibration-" in name
                and f"-{polarization.lower()}-" in name
                and name.endswith(".xml")
            ):
                return name
    raise FileNotFoundError(
        f"No calibration XML for polarization {polarization!r} in {zip_path.name}"
    )


# Mapping from the canonical short-name we accept on the CLI to the
# actual element name in the calibration XML's <calibrationVector>.
# Add gamma / dn here if a consumer ever needs them.
LUT_ELEMENT_BY_CALIBRATION = {
    "sigma0": "sigmaNought",
    "beta0":  "betaNought",
    "gamma0": "gamma",
}


def parse_calibration_lut(zip_path: Path, calibration_xml_path: str,
                          calibration: str = "sigma0") -> Tuple[
    np.ndarray, np.ndarray, np.ndarray,
]:
    """Read one calibration LUT from `calibration_xml_path` inside the
    SAFE ZIP. `calibration` is one of `sigma0` / `beta0` / `gamma0`.

    Returns `(lines, pixels, lut)`:
      - lines: 1-D array of length L (image row coords of LUT samples).
      - pixels: 1-D array of length P (image col coords; for GRD this is
        constant across all LUT rows, so we read it from the first row).
      - lut: 2-D array of shape (L, P) — k(i,j) values for the chosen
        calibration convention.
    """
    if calibration not in LUT_ELEMENT_BY_CALIBRATION:
        raise ValueError(
            f"Unknown calibration {calibration!r}; "
            f"valid: {list(LUT_ELEMENT_BY_CALIBRATION)}"
        )
    element = LUT_ELEMENT_BY_CALIBRATION[calibration]

    with zipfile.ZipFile(str(zip_path)) as zf:
        xml_bytes = zf.read(calibration_xml_path)

    tree = ET.fromstring(xml_bytes)
    vectors = tree.findall(".//calibrationVector")
    if not vectors:
        raise RuntimeError(
            f"No <calibrationVector> nodes in {calibration_xml_path}"
        )

    lines = np.array([int(v.find("line").text) for v in vectors])
    pixels = np.array([int(x) for x in vectors[0].find("pixel").text.split()])
    lut = np.array([
        [float(x) for x in v.find(element).text.split()]
        for v in vectors
    ])
    if lut.shape != (len(lines), len(pixels)):
        raise RuntimeError(
            f"Calibration LUT shape mismatch for {calibration}: "
            f"lines={len(lines)}, pixels={len(pixels)}, lut={lut.shape}"
        )
    return lines, pixels, lut


def apply_calibration(dn: np.ndarray, lines: np.ndarray, pixels: np.ndarray,
                      lut: np.ndarray) -> np.ndarray:
    """Apply DN²/k² calibration to a 2-D DN array and convert to **decibels**.

    The DN²/k² formula is identical across σ⁰ / β⁰ / γ⁰ conventions —
    only the LUT (`k`) differs. The result is then transformed to dB
    via `10 * log10(linear)` so the output COG carries directly-
    displayable radar units (no layer-side `10*log10(b1)` needed for
    MMGIS / TiTiler). DN values of 0 map to NaN (no-signal / off-swath).
    Returns a Float32 array of the same shape as `dn`, in dB.
    """
    height, width = dn.shape
    interp = RegularGridInterpolator(
        (lines, pixels), lut,
        method="linear", bounds_error=False, fill_value=None,
    )
    yy, xx = np.meshgrid(np.arange(height), np.arange(width), indexing="ij")
    k = interp((yy, xx)).astype(np.float32)
    # `np.where(dn > 0, ..., nan)` masks off-swath / no-signal pixels;
    # log10 propagates NaN cleanly. Suppress numpy's well-meaning
    # divide-by-zero / invalid warnings (we already masked the zeros).
    linear = np.where(dn > 0, (dn ** 2) / (k ** 2), np.nan).astype(np.float32)
    with np.errstate(divide="ignore", invalid="ignore"):
        return (10.0 * np.log10(linear)).astype(np.float32)


def read_uncalib_dn(zip_path: Path, polarization: str,
                    swath: str = "EW") -> Tuple[np.ndarray, tuple, int, int]:
    """Open the SAFE bundle's UNCALIB subdataset for `polarization`,
    read the raw DN band, and return `(dn, gcps, height, width)`.

    Separated so callers that need multiple calibrations off the same
    granule only pay the GDAL open + read cost once.
    """
    uncalib_uri = (
        f"SENTINEL1_CALIB:UNCALIB:/vsizip/{zip_path}/"
        f"{zip_path.stem}.SAFE/manifest.safe:"
        f"{swath}_{polarization.upper()}:AMPLITUDE"
    )
    with rasterio.open(uncalib_uri) as src:
        dn = src.read(1).astype(np.float32)
        gcps = src.gcps  # (list_of_GCP, CRS)
        height, width = src.shape
    return dn, gcps, height, width


def write_calibrated_tiff(values: np.ndarray, gcps: tuple,
                          output_path: Path) -> Path:
    """Write a Float32 GeoTIFF that preserves the source's GCPs so a
    downstream gdalwarp can geocode it."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    height, width = values.shape
    profile = {
        "driver": "GTiff",
        "dtype": "float32",
        "count": 1,
        "width": width,
        "height": height,
        "nodata": np.nan,
        "compress": "deflate",
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
    }
    with rasterio.open(str(output_path), "w", **profile) as dst:
        dst.write(values, 1)
        dst.gcps = (gcps[0], gcps[1])
    return output_path


def calibrate_granule(zip_path: Path, polarization: str,
                      output_path: Path,
                      calibration: str = "sigma0",
                      swath: str = "EW") -> Path:
    """Single-calibration convenience wrapper — opens DN, parses LUT,
    applies, writes. For multi-calibration runs (e.g. σ⁰ AND β⁰ off
    the same granule) use `read_uncalib_dn` + `parse_calibration_lut` +
    `apply_calibration` + `write_calibrated_tiff` directly so the
    DN read happens only once.
    """
    dn, gcps, _, _ = read_uncalib_dn(zip_path, polarization, swath=swath)
    cal_xml = find_calibration_xml(zip_path, polarization.lower())
    lines, pixels, lut = parse_calibration_lut(zip_path, cal_xml, calibration)
    calibrated = apply_calibration(dn, lines, pixels, lut)
    return write_calibrated_tiff(calibrated, gcps, output_path)


# Backward-compat aliases for any caller still using the σ⁰-only API.
def parse_sigma_lut(zip_path: Path, calibration_xml_path: str):
    return parse_calibration_lut(zip_path, calibration_xml_path, "sigma0")


def apply_sigma0(dn, lines, pixels, sigma_lut):
    return apply_calibration(dn, lines, pixels, sigma_lut)
