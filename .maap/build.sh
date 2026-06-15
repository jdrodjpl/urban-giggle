#!/usr/bin/env bash
# Top-level build entry point.
#
# Not used directly by MAAP DPS — each algorithm registers with its own
# build_command pointing at .maap/<algo>/build.sh, which installs that
# algorithm's deps via `python3 -m pip install -r requirements.txt`.
#
# Left in place so local developers can do a "build everything" sanity
# check; no-op if requirements files don't exist.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

for algo in ingest-cog ingest-zarr zarr-pipeline; do
    req="${basedir}/${algo}/requirements.txt"
    if [[ -f "${req}" ]]; then
        echo "==> Installing ${algo} deps from ${req}"
        python3 -m pip install -r "${req}"
    fi
done
