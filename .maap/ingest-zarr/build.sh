#!/usr/bin/env bash
# Build script for the Frozon Zarr ingest worker.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Building Frozon ISS Zarr ingest worker environment..."
pushd "${basedir}"
conda env update -n ingest --file environment.yml
popd

conda run -n ingest pip install jq
echo "Build complete!"
