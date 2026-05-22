#!/usr/bin/env bash
# Build script for the Frozon Zarr ingest worker. No conda — pip only.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

echo "Building Frozon ISS Zarr ingest worker environment..."

if ! command -v jq >/dev/null 2>&1; then
    apt-get update && apt-get install -y --no-install-recommends jq && rm -rf /var/lib/apt/lists/*
fi

echo "PYTHON: $(which python3) ($(python3 --version 2>&1))"
echo "JQ:     $(which jq) ($(jq --version))"
echo "PIP:    $(which pip3 2>/dev/null || echo 'pip3 not on PATH; using python3 -m pip')"

python3 -m pip install --upgrade pip
python3 -m pip install -r "${basedir}/requirements.txt"

# Smoke import — fail the build, not the runtime, if a hard dep is missing.
python3 -c "import zarr, xarray, rioxarray, rasterio, s3fs, earthaccess; print('deps OK:', zarr.__version__)"

echo "Build complete!"
