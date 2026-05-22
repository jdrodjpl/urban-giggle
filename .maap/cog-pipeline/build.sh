#!/usr/bin/env bash
# Build script for the Frozon COG pipeline orchestrator. No conda — pip only.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

echo "Building Frozon ISS COG pipeline orchestrator environment..."
echo "PYTHON: $(which python3) ($(python3 --version 2>&1))"

python3 -m pip install --upgrade pip
python3 -m pip install -r "${basedir}/requirements.txt"

python3 -c "import pystac, boto3, backoff, earthaccess; print('deps OK')"

echo "Build complete!"
