#!/usr/bin/env bash
# Build script for the Frozon Zarr pipeline orchestrator.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Building Frozon ISS Zarr pipeline orchestrator environment..."
pushd "${basedir}"
conda env update -n ingest --file environment.yml
popd

conda run -n ingest pip install jq
echo "Build complete!"
