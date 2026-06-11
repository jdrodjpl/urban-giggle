#!/usr/bin/env bash
# Build script for the Frozon COG pipeline orchestrator. No conda — pip only.
# BUILD_BUST=2026-06-10-1  ← bump to force a fresh Docker build (cache hit
#                            on algorithm_version=main otherwise re-uses
#                            the prior image even when source changed).
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

echo "Building Frozon ISS COG pipeline orchestrator environment..."

echo "PYTHON: $(which python3) ($(python3 --version 2>&1))"
# jq is no longer required — run-cog-pipeline.sh uses python's json
# stdlib via .maap/_lib/load_job_params.py.

python3 -m pip install --upgrade pip
python3 -m pip install -r "${basedir}/requirements.txt"

python3 -c "import pystac, boto3, backoff, earthaccess; from maap.dps.dps_job import DPSJob; print('deps OK')"

# Stamp the image so runtime diagnostics can prove which build we're on.
{
    echo "build_date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_sha: ${CI_COMMIT_SHA:-unknown}"
    echo "git_ref: ${CI_COMMIT_REF_NAME:-unknown}"
    echo "maap_py: $(python3 -m pip show maap-py | head -2 | tr '\n' ' ')"
} > "${basedir}/.build-stamp"
echo "=== Build stamp ==="
cat "${basedir}/.build-stamp"
echo "==================="

echo "Build complete!"
