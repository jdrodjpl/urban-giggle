#!/usr/bin/env bash
# Build script for the COG ingest worker.
# BUILD_BUST=2026-06-02-2  ← bump to force a fresh Docker build.
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
# conda env update only updates an EXISTING env. On a fresh BUILD_BUST'd
# image the 'ingest' env doesn't exist yet, so try create first and fall
# through to update for incremental builds.
if conda env list | awk '{print $1}' | grep -qx ingest; then
    conda env update -n ingest --file environment.yml
else
    conda env create -n ingest --file environment.yml
fi
popd

# Smoke import — fail the build, not the runtime, if a hard dep is missing.
conda run -n ingest python -c "import rasterio, rioxarray, pystac, rio_stac, boto3, earthaccess, paramiko, scp; print('deps OK')"

echo "Build complete!"
