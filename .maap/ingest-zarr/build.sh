#!/usr/bin/env bash
# Build script for the Zarr ingest worker.
#
# Uses conda env update because the geospatial stack (rasterio, gdal,
# rioxarray, pyproj, zarr) needs coordinated C-library versions that
# pip wheels don't reliably provide. Orchestrators stay on pip.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

echo "Building Frozon ISS Zarr ingest worker environment..."
pushd "${basedir}"
if conda env list | awk '{print $1}' | grep -qx ingest; then
    conda env update -n ingest --file environment.yml
else
    conda env create -n ingest --file environment.yml
fi
popd

# Smoke import — fail the build, not the runtime.
conda run -n ingest python -c "import zarr, xarray, rioxarray, rasterio, s3fs, earthaccess; print('deps OK:', zarr.__version__)"

echo "Build complete!"
