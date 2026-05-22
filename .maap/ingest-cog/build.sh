#!/usr/bin/env bash
# Build script for the Frozon COG ingest worker. No conda — pip only.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

echo "Building Frozon ISS COG ingest worker environment..."

if ! command -v jq >/dev/null 2>&1; then
    apt-get update && apt-get install -y --no-install-recommends jq && rm -rf /var/lib/apt/lists/*
fi

echo "PYTHON: $(which python3) ($(python3 --version 2>&1))"
echo "JQ:     $(which jq) ($(jq --version))"

python3 -m pip install --upgrade pip
python3 -m pip install -r "${basedir}/requirements.txt"

# Smoke import — fail the build, not the runtime, if a hard dep is missing.
python3 -c "import rasterio, rioxarray, pystac, rio_stac, boto3, earthaccess, paramiko, scp; print('deps OK')"

echo "Build complete!"
