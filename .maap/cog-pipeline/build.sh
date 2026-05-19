#!/usr/bin/env bash
# Build script for the Frozon COG pipeline algorithm on MAAP.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Building Frozon ISS COG pipeline environment..."
pushd "${basedir}"
conda env update -n ingest --file environment.yml
popd

# For input parsing in the run scripts.
conda run -n ingest pip install jq
echo "Build complete!"
