#!/usr/bin/env bash
# Build script for the COG ingest worker.
# BUILD_BUST=2026-06-11-1  ← bump to force a fresh Docker build.
#
# Uses conda env update because the geospatial stack (rasterio, gdal,
# rioxarray, pyproj) needs coordinated C-library versions that pip
# wheels don't reliably provide. The orchestrators (which only need
# maap-py, pystac, boto3) stay on pip — this is the only place we
# need conda.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )

# Runtime expects /opt/conda/envs/ingest/bin/python. Pin the env path
# explicitly via --prefix so the build can't silently put it in a
# user-home conda location that the runtime image won't see.
INGEST_ENV_PREFIX=/opt/conda/envs/ingest

echo "Building Frozon ISS COG ingest worker environment..."
echo "=== Pre-build conda state ==="
echo "conda binary: $(command -v conda)"
conda info || true
echo "ls /opt/conda/envs/ before:"; ls -la /opt/conda/envs/ 2>&1 || echo "  /opt/conda/envs does not exist"
echo "============================="

pushd "${basedir}"
# Try create first; if env already exists, fall back to update.
# --prefix anchors the env to /opt/conda/envs/ingest regardless of where
# this build's conda would otherwise default to.
conda env create --prefix "${INGEST_ENV_PREFIX}" --file environment.yml \
  || conda env update --prefix "${INGEST_ENV_PREFIX}" --file environment.yml
popd

echo "=== Post-build conda state ==="
echo "ls /opt/conda/envs/ after:"; ls -la /opt/conda/envs/ 2>&1 || echo "  /opt/conda/envs still missing"
conda env list || true
echo "==============================="

# Hard-verify the SPECIFIC path the runtime looks at. The previous check
# (`conda env list | grep ingest`) was satisfied by any conda location,
# even one the runtime image can't see.
if [[ ! -x "${INGEST_ENV_PREFIX}/bin/python" ]]; then
    echo "FATAL: ${INGEST_ENV_PREFIX}/bin/python not found after build"
    echo "  /opt/conda/envs/ contents:"
    ls -la /opt/conda/envs/ 2>&1 || true
    echo "  any 'ingest' env on disk:"
    find / -maxdepth 6 -type d -name ingest 2>/dev/null | head -10 || true
    exit 1
fi

# Smoke import — fail the build, not the runtime, if a hard dep is missing.
"${INGEST_ENV_PREFIX}/bin/python" -c "import rasterio, rioxarray, pystac, rio_stac, boto3, earthaccess, paramiko, scp; print('deps OK')"

# Stamp the image so runtime diagnostics can prove which build we're on.
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
