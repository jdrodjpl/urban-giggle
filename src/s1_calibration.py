"""Sentinel-1 GRD radiometric calibration.

GDAL's SAFE driver exposes the raw DN ("UNCALIB") subdatasets but
doesn't apply σ⁰ / β⁰ / γ⁰ calibration for GRD products in any current
version. This module applies σ⁰ ourselves from the calibration LUT in
the SAFE bundle:

    σ⁰(i,j) = DN(i,j)² / k(i,j)²

where k(i,j) is the `sigmaNought` value from the calibration vector
grid, bilinearly interpolated to every pixel.

Output is linear σ⁰ (Float32). NaN for pixels where the input was 0
(no signal / off-swath).

References:
- ESA Sentinel-1 Product Specification, Section 9.2 (calibration)
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


def parse_sigma_lut(zip_path: Path, calibration_xml_path: str) -> Tuple[
    np.ndarray, np.ndarray, np.ndarray,
]:
    """Read the σ⁰ calibration LUT from `calibration_xml_path` inside
    the SAFE ZIP. Returns (lines, pixels, sigmaNought) where:
      - lines: 1-D array of length L (image row coords of LUT samples)
      - pixels: 1-D array of length P (image col coords; assumed
        constant across all LUT rows for GRD products)
      - sigmaNought: 2-D array of shape (L, P) (k(i,j) calibration values)
    """
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
    sigma = np.array([
        [float(x) for x in v.find("sigmaNought").text.split()]
        for v in vectors
    ])
    if sigma.shape != (len(lines), len(pixels)):
        raise RuntimeError(
            f"Calibration LUT shape mismatch: lines={len(lines)}, "
            f"pixels={len(pixels)}, sigma={sigma.shape}"
        )
    return lines, pixels, sigma


def apply_sigma0(dn: np.ndarray, lines: np.ndarray, pixels: np.ndarray,
                 sigma_lut: np.ndarray) -> np.ndarray:
    """Apply σ⁰ calibration to a 2-D DN array using the parsed LUT.

    Interpolates the LUT bilinearly to every pixel, then computes
    σ⁰ = DN² / k². DN values of 0 map to NaN (no-signal / off-swath).
    Returns a Float32 array of the same shape as `dn`.
    """
    height, width = dn.shape
    interp = RegularGridInterpolator(
        (lines, pixels), sigma_lut,
        method="linear", bounds_error=False, fill_value=None,
    )
    yy, xx = np.meshgrid(np.arange(height), np.arange(width), indexing="ij")
    k = interp((yy, xx)).astype(np.float32)
    return np.where(dn > 0, (dn ** 2) / (k ** 2), np.nan).astype(np.float32)


def calibrate_granule(zip_path: Path, polarization: str,
                      output_path: Path,
                      swath: str = "EW") -> Path:
    """Read a single S1 GRD granule's DN band via GDAL's SAFE driver,
    apply σ⁰ calibration, and write a Float32 GeoTIFF with the original
    GCPs preserved (so gdalwarp can reproject it).

    `polarization` is "HH" / "HV" / "VV" / "VH". `swath` is "EW" / "IW".
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

    cal_xml = find_calibration_xml(zip_path, polarization.lower())
    lines, pixels, sigma_lut = parse_sigma_lut(zip_path, cal_xml)
    sigma0 = apply_sigma0(dn, lines, pixels, sigma_lut)

    output_path.parent.mkdir(parents=True, exist_ok=True)
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
        dst.write(sigma0, 1)
        dst.gcps = (gcps[0], gcps[1])

    return output_path
