#!/usr/bin/env bash
# Build script for the Sentinel-1 GRD ingest worker.
# BUILD_BUST=2026-06-29-1  ← bump to force a fresh Docker build.
#   (Deployed v1 ran current worker source on a stale conda layer missing
#   scipy; bumping forces conda env create/update to re-run. Smoke-import gates.)
#
# Same conda env shape as the OPERA worker (rasterio + gdal + boto3 +
# earthaccess) plus scipy (for RegularGridInterpolator in the σ⁰
# calibration step) and asf-search (ASF-aware downloader for the
# SAFE ZIPs).
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

INGEST_ENV_PREFIX=/opt/conda/envs/ingest

echo "Building Frozon Sentinel-1 GRD ingest worker environment..."
echo "=== Pre-build conda state ==="
echo "conda binary: $(command -v conda)"
conda info || true
echo "ls /opt/conda/envs/ before:"; ls -la /opt/conda/envs/ 2>&1 || echo "  /opt/conda/envs does not exist"
echo "============================="

pushd "${basedir}"
conda env create --prefix "${INGEST_ENV_PREFIX}" --file environment.yml \
  || conda env update --prefix "${INGEST_ENV_PREFIX}" --file environment.yml
popd

echo "=== Post-build conda state ==="
echo "ls /opt/conda/envs/ after:"; ls -la /opt/conda/envs/ 2>&1 || echo "  /opt/conda/envs still missing"
conda env list || true
echo "==============================="

if [[ ! -x "${INGEST_ENV_PREFIX}/bin/python" ]]; then
    echo "FATAL: ${INGEST_ENV_PREFIX}/bin/python not found after build"
    ls -la /opt/conda/envs/ 2>&1 || true
    find / -maxdepth 6 -type d -name ingest 2>/dev/null | head -10 || true
    exit 1
fi

# Smoke import. asf_search + scipy are the new ones vs the OPERA worker.
"${INGEST_ENV_PREFIX}/bin/python" -c "
import rasterio, rioxarray, pystac, rio_stac, boto3, earthaccess, paramiko, scp
import scipy.interpolate
import asf_search
print('deps OK')
"

{
    echo "build_date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_sha: ${CI_COMMIT_SHA:-unknown}"
    echo "git_ref: ${CI_COMMIT_REF_NAME:-unknown}"
    echo "env_path: ${INGEST_ENV_PREFIX}"
    echo "python: $(${INGEST_ENV_PREFIX}/bin/python --version)"
} > "${basedir}/.build-stamp"
echo "=== Build stamp ==="
cat "${basedir}/.build-stamp"
echo "==================="

echo "Build complete!"
