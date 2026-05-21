#!/usr/bin/env bash
# Build script for the Frozon Zarr ingest worker.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Building Frozon ISS Zarr ingest worker environment..."
pushd "${basedir}"
conda env update -n ingest --file environment.yml
popd

# Backstop installs: 'conda env update' has been observed to silently skip
# new conda-channel packages on existing envs, so pin the must-have geospatial
# stack via pip as a belt-and-suspenders.
conda run -n ingest pip install jq zarr xarray rioxarray rasterio s3fs

# Verify the worker's hard dependencies actually made it in. Fail loud if
# anything's missing rather than letting the job die at runtime.
conda run -n ingest python -c "import zarr, xarray, rioxarray, rasterio, s3fs, earthaccess; print('deps OK:', zarr.__version__)"

echo "Build complete!"
