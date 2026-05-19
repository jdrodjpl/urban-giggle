#!/usr/bin/env bash
# Top-level build entry point. Mirrors czdt-iss-ingest-job/.maap/build.sh.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname "${basedir}")

pushd "${root_dir}"
conda env update -f environment.yml
popd
