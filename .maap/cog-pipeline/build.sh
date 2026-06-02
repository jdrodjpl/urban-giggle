#!/usr/bin/env bash
# Build script for the Frozon COG pipeline orchestrator. No conda — pip only.
# BUILD_BUST=2026-06-02-4  ← bump to force a fresh Docker build (cache hit
#                            on algorithm_version=main otherwise re-uses
#                            the prior image even when source changed).
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

echo "Building Frozon ISS COG pipeline orchestrator environment..."

apt-get update && apt-get install -y --no-install-recommends jq && rm -rf /var/lib/apt/lists/*

echo "PYTHON: $(which python3) ($(python3 --version 2>&1))"
echo "JQ:     $(which jq) ($(jq --version))"
echo "JQ-path-check: $(ls -la $(which jq))"

python3 -m pip install --upgrade pip
python3 -m pip install -r "${basedir}/requirements.txt"

python3 -c "import pystac, boto3, backoff, earthaccess; print('deps OK')"

echo "Build complete!"
