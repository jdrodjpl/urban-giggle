#!/usr/bin/env bash
# Build script for the CMEMS ocean-current ingest worker.
# BUILD_BUST=2026-07-22-1  ← bump to force a fresh Docker build.
#   (Re-registering v1 kept serving a stale image missing rioxarray — the
#   smoke import below can't fail on a real build. MAAP keys images per
#   repo:version and skips the build when that version's image exists, so the
#   algo moved to v8, a version never used by any algo in this repo. See
#   PIPELINE_TEMPLATE.md gotcha 16.)
#
# Credentialed Copernicus Marine subset (copernicusmarine) + xarray/rioxarray
# NetCDF->GeoTIFF + gdalwarp/gdal_translate for the warp + COG. No GDAL netCDF
# plugin needed (NetCDF is read via xarray/netcdf4).
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

INGEST_ENV_PREFIX=/opt/conda/envs/ingest

echo "Building Frozon CMEMS ingest worker environment..."
echo "=== Pre-build conda state ==="
echo "conda binary: $(command -v conda)"
conda info || true
echo "ls /opt/conda/envs/ before:"; ls -la /opt/conda/envs/ 2>&1 || echo "  /opt/conda/envs does not exist"
echo "============================="

pushd "${basedir}"
conda env create --prefix "${INGEST_ENV_PREFIX}" --file environment.yml \
  || conda env update --prefix "${INGEST_ENV_PREFIX}" --file environment.yml
popd

echo "=== Post-build conda state ==="
echo "ls /opt/conda/envs/ after:"; ls -la /opt/conda/envs/ 2>&1 || echo "  /opt/conda/envs still missing"
conda env list || true
echo "==============================="

if [[ ! -x "${INGEST_ENV_PREFIX}/bin/python" ]]; then
    echo "FATAL: ${INGEST_ENV_PREFIX}/bin/python not found after build"
    ls -la /opt/conda/envs/ 2>&1 || true
    exit 1
fi

# Smoke import — everything the worker + reused cog_helpers touch. Assert the
# copernicusmarine toolbox imports and the rioxarray .rio accessor registers.
"${INGEST_ENV_PREFIX}/bin/python" -c "
import rasterio, numpy, pystac, rio_stac, boto3, requests
import xarray as xr, rioxarray
import copernicusmarine
from osgeo import gdal
assert hasattr(xr.DataArray, 'rio'), 'rioxarray .rio accessor not registered'
print('deps OK; copernicusmarine', getattr(copernicusmarine, '__version__', '?'),
      '; rioxarray accessor present; gdal', gdal.__version__)
"

{
    echo "build_date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_sha: ${CI_COMMIT_SHA:-unknown}"
    echo "git_ref: ${CI_COMMIT_REF_NAME:-unknown}"
    echo "env_path: ${INGEST_ENV_PREFIX}"
    echo "python: $(${INGEST_ENV_PREFIX}/bin/python --version)"
} > "${basedir}/.build-stamp"
echo "=== Build stamp ==="
cat "${basedir}/.build-stamp"
echo "==================="

echo "Build complete!"
