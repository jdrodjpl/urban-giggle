#!/usr/bin/env bash
# Build script for the Frozon Zarr pipeline orchestrator. No conda — pip only.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

echo "Building Frozon ISS Zarr pipeline orchestrator environment..."

# jq is used by run-*.sh to read _job.json. Apt-install at build time so
# it's baked into the image; the build runs as root so no sudo needed.
apt-get update && apt-get install -y --no-install-recommends jq && rm -rf /var/lib/apt/lists/*

echo "PYTHON: $(which python3) ($(python3 --version 2>&1))"
echo "JQ:     $(which jq) ($(jq --version))"

python3 -m pip install --upgrade pip
python3 -m pip install -r "${basedir}/requirements.txt"

python3 -c "import pystac, boto3, backoff, earthaccess; print('deps OK')"

echo "Build complete!"
