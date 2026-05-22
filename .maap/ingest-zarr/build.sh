#!/usr/bin/env bash
# Build script for the Frozon Zarr ingest worker.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Building Frozon ISS Zarr ingest worker environment..."
pushd "${basedir}"
conda env update -n ingest --file environment.yml
popd

# Activate the env explicitly so subsequent `python` / `pip` resolve to the
# env's interpreter and site-packages. Bypass `conda run` which has been
# observed to install packages into the base env on this base image.
source activate ingest

echo "=== Environment debug ==="
echo "PYTHON: $(which python) ($(python --version 2>&1))"
echo "PIP:    $(which pip)"
echo "CONDA_PREFIX: ${CONDA_PREFIX:-unset}"
echo "========================="

# Make absolutely sure pip lives in this env, then install the must-have
# geospatial stack here. `python -m pip` is the canonical way to ensure
# the pip installer maps to *this* python's site-packages.
python -m pip install --upgrade pip
python -m pip install jq zarr xarray rioxarray rasterio s3fs earthaccess

# Verify the worker's hard dependencies actually made it in. Fail loud if
# anything's missing rather than letting the job die at runtime.
python -c "import zarr, xarray, rioxarray, rasterio, s3fs, earthaccess; print('deps OK:', zarr.__version__)"

echo "Build complete!"
