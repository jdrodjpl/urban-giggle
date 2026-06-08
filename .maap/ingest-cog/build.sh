#!/usr/bin/env bash
# Build script for the COG ingest worker.
# BUILD_BUST=2026-06-08-3  ← bump to force a fresh Docker build.
#
# Uses conda env update because the geospatial stack (rasterio, gdal,
# rioxarray, pyproj) needs coordinated C-library versions that pip
# wheels don't reliably provide. The orchestrators (which only need
# maap-py, pystac, boto3) stay on pip — this is the only place we
# need conda.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

echo "Building Frozon ISS COG ingest worker environment..."
echo "=== Pre-build conda state ==="
conda info --envs || true
echo "============================="

pushd "${basedir}"
# Try create first; if env already exists, fall back to update.
# This is more reliable than parsing `conda env list`.
conda env create -n ingest --file environment.yml \
  || conda env update -n ingest --file environment.yml
popd

echo "=== Post-build conda state ==="
conda info --envs || true
echo "==============================="

# Hard-verify the env exists before declaring success.
if ! conda env list | awk '{print $1}' | grep -qx ingest; then
    echo "FATAL: ingest env still missing after build"
    conda env list
    exit 1
fi

# Smoke import — fail the build, not the runtime, if a hard dep is missing.
conda run -n ingest python -c "import rasterio, rioxarray, pystac, rio_stac, boto3, earthaccess, paramiko, scp; print('deps OK')"

echo "Build complete!"
