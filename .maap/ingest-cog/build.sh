#!/usr/bin/env bash
# Build script for the per-TIFF Frozon COG ingest worker.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Building Frozon ISS COG ingest worker environment..."
pushd "${basedir}"
conda env update -n ingest --file environment.yml
popd

conda run -n ingest pip install jq
echo "Build complete!"
