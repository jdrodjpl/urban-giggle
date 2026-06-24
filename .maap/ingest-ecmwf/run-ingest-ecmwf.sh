#!/usr/bin/env bash
# Frozon ECMWF Open Data near-surface ingest worker runner.
# Reads job parameters from _job.json and invokes src/ingest_ecmwf.py
# for one (product, date).
set -eo pipefail
set +u

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running Frozon ECMWF ingest worker..."

echo "=== Runtime conda discovery ==="
echo "ls /opt/conda/envs/:"; ls -la /opt/conda/envs/ 2>&1 || echo "  /opt/conda/envs not found"
echo "image build stamp:"
cat "${root_dir}/.maap/ingest-ecmwf/.build-stamp" 2>/dev/null || echo "  (no .build-stamp — image is pre-stamp)"
echo "================================"

if [[ ! -x /opt/conda/envs/ingest/bin/python ]]; then
    echo "FATAL: /opt/conda/envs/ingest/bin/python not found." >&2
    exit 1
fi
ENV_PYTHON=/opt/conda/envs/ingest/bin/python
echo "Using ${ENV_PYTHON}"

export PROJ_DATA=/opt/conda/envs/ingest/share/proj
export PROJ_LIB=/opt/conda/envs/ingest/share/proj
export GDAL_DATA=/opt/conda/envs/ingest/share/gdal
# conda-forge GDAL 3.9+ ships format drivers (GRIB, netCDF, …) as separate
# plugin packages under <env>/lib/gdalplugins/. GDAL only auto-scans there when
# activated via `conda activate` — we bypass activation, so point
# GDAL_DRIVER_PATH at it explicitly or rasterio errors with "plugin gdal_GRIB.so
# is not available".
export GDAL_DRIVER_PATH=/opt/conda/envs/ingest/lib/gdalplugins
export PATH=/opt/conda/envs/ingest/bin:${PATH}
echo "PROJ_DATA=${PROJ_DATA}"
echo "GDAL_DATA=${GDAL_DATA}"
echo "GDAL_DRIVER_PATH=${GDAL_DRIVER_PATH}"

if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

# Pre-declare all vars.
product="" date="" time="0" step="0" resol="0p25"
collection_id="" s3_bucket="" s3_prefix="" role_arn=""
compress="DEFLATE" blocksize="512" max_memory="512" overwrite="false"

eval "$(/opt/conda/envs/ingest/bin/python "${root_dir}/.maap/_lib/load_job_params.py" _job.json)"

echo "=== Parsed parameters ==="
echo "product:        ${product}"
echo "date:           ${date}"
echo "time/step:      ${time} / ${step}"
echo "resol:          ${resol}"
echo "collection_id:  ${collection_id}"
echo "s3_bucket:      ${s3_bucket}"
echo "s3_prefix:      ${s3_prefix}"
echo "compress/block/mem: ${compress} / ${blocksize} / ${max_memory}"
echo "========================="

args=()
[[ -n "${product}" ]]        && args+=(--product "${product}")
[[ -n "${date}" ]]           && args+=(--date "${date}")
[[ -n "${time}" ]]           && args+=(--time "${time}")
[[ -n "${step}" ]]           && args+=(--step "${step}")
[[ -n "${resol}" ]]          && args+=(--resol "${resol}")
[[ -n "${collection_id}" ]]  && args+=(--collection-id "${collection_id}")
[[ -n "${s3_bucket}" ]]      && args+=(--s3-bucket "${s3_bucket}")
[[ -n "${s3_prefix}" ]]      && args+=(--s3-prefix "${s3_prefix}")
[[ -n "${role_arn}" ]]       && args+=(--role-arn "${role_arn}")
args+=(--compress "${compress}")
args+=(--blocksize "${blocksize}")
args+=(--max-memory "${max_memory}")
args+=(--output output)
[[ "${overwrite}" == "true" ]] && args+=(--overwrite)

worker_script="${root_dir}/src/ingest_ecmwf.py"
echo "Executing: python ${worker_script} ${args[@]}"
"${ENV_PYTHON}" "${worker_script}" "${args[@]}"
