#!/usr/bin/env bash
# Build script for the ECMWF Open Data near-surface ingest worker.
# BUILD_BUST=2026-07-07-1  ← bump to force a fresh Docker build.
#
# Same lean shape as the OSI SAF worker: anonymous HTTP (no earthaccess/EDL),
# GRIB-direct via GDAL. Adds the ecmwf-opendata client and the GDAL GRIB plugin.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

INGEST_ENV_PREFIX=/opt/conda/envs/ingest

echo "Building Frozon ECMWF ingest worker environment..."
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
    find / -maxdepth 6 -type d -name ingest 2>/dev/null | head -10 || true
    exit 1
fi

# Smoke import — everything the worker + reused cog_helpers touch.
# Assert the GRIB GDAL driver is present: conda-forge GDAL 3.9+ ships it as a
# separate plugin (libgdal-grib), and without it rasterio.open() on the ECMWF
# GRIB2 fails at runtime. Also confirm the ecmwf-opendata client imports.
"${INGEST_ENV_PREFIX}/bin/python" -c "
import rasterio, numpy, pystac, rio_stac, boto3, requests
from osgeo import gdal
from ecmwf.opendata import Client
assert gdal.GetDriverByName('GRIB') is not None, 'GRIB GDAL driver missing — libgdal-grib not installed'
print('deps OK; GRIB driver present; ecmwf-opendata Client importable')
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
