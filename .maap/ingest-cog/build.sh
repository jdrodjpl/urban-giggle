#!/usr/bin/env bash
# Build script for the COG ingest worker.
#
# Uses conda env update because the geospatial stack (rasterio, gdal,
# rioxarray, pyproj) needs coordinated C-library versions that pip
# wheels don't reliably provide. The orchestrators (which only need
# maap-py, pystac, boto3) stay on pip — this is the only place we
# need conda.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

echo "Building Frozon ISS COG ingest worker environment..."
pushd "${basedir}"
conda env update -n ingest --file environment.yml
popd

# Smoke import — fail the build, not the runtime, if a hard dep is missing.
conda run -n ingest python -c "import rasterio, rioxarray, pystac, rio_stac, boto3, earthaccess, paramiko, scp; print('deps OK')"

echo "Build complete!"
