#!/usr/bin/env bash
# Frozon CMEMS ocean-current ingest worker runner.
# Reads job parameters from _job.json and invokes src/ingest_cmems.py
# for one (product, date).
set -eo pipefail
set +u

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running Frozon CMEMS ingest worker..."

echo "=== Runtime conda discovery ==="
echo "ls /opt/conda/envs/:"; ls -la /opt/conda/envs/ 2>&1 || echo "  /opt/conda/envs not found"
echo "image build stamp:"
cat "${root_dir}/.maap/ingest-cmems/.build-stamp" 2>/dev/null || echo "  (no .build-stamp — image is pre-stamp)"
echo "================================"

if [[ ! -x /opt/conda/envs/ingest/bin/python ]]; then
    echo "FATAL: /opt/conda/envs/ingest/bin/python not found." >&2
    exit 1
fi
ENV_PYTHON=/opt/conda/envs/ingest/bin/python
echo "Using ${ENV_PYTHON}"

# We bypass `conda activate`, so point PROJ/GDAL at the env explicitly or
# gdalwarp/gdal_translate can't find their data files.
export PROJ_DATA=/opt/conda/envs/ingest/share/proj
export PROJ_LIB=/opt/conda/envs/ingest/share/proj
export GDAL_DATA=/opt/conda/envs/ingest/share/gdal
export GDAL_DRIVER_PATH=/opt/conda/envs/ingest/lib/gdalplugins
export PATH=/opt/conda/envs/ingest/bin:${PATH}
echo "PROJ_DATA=${PROJ_DATA}"
echo "GDAL_DATA=${GDAL_DATA}"

if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

# Pre-declare all vars.
product="" date="" dataset_id="" cmems_secret_name="" bbox=""
min_depth="" max_depth=""
collection_id="" s3_bucket="" s3_prefix="" role_arn=""
compress="DEFLATE" blocksize="512" max_memory="512" overwrite="false"

eval "$(/opt/conda/envs/ingest/bin/python "${root_dir}/.maap/_lib/load_job_params.py" _job.json)"

echo "=== Parsed parameters ==="
echo "product:           ${product}"
echo "date:              ${date}"
echo "dataset_id:        ${dataset_id}"
echo "cmems_secret_name: ${cmems_secret_name}"
echo "bbox:              ${bbox}"
echo "depth:             ${min_depth} .. ${max_depth}"
echo "collection_id:     ${collection_id}"
echo "s3_bucket:         ${s3_bucket}"
echo "s3_prefix:         ${s3_prefix}"
echo "compress/block/mem: ${compress} / ${blocksize} / ${max_memory}"
echo "========================="

args=()
[[ -n "${product}" ]]            && args+=(--product "${product}")
[[ -n "${date}" ]]               && args+=(--date "${date}")
[[ -n "${dataset_id}" ]]         && args+=(--dataset-id "${dataset_id}")
[[ -n "${cmems_secret_name}" ]]  && args+=(--cmems-secret-name "${cmems_secret_name}")
[[ -n "${bbox}" ]]               && args+=(--bbox "${bbox}")
[[ -n "${min_depth}" ]]          && args+=(--min-depth "${min_depth}")
[[ -n "${max_depth}" ]]          && args+=(--max-depth "${max_depth}")
[[ -n "${collection_id}" ]]      && args+=(--collection-id "${collection_id}")
[[ -n "${s3_bucket}" ]]          && args+=(--s3-bucket "${s3_bucket}")
[[ -n "${s3_prefix}" ]]          && args+=(--s3-prefix "${s3_prefix}")
[[ -n "${role_arn}" ]]           && args+=(--role-arn "${role_arn}")
args+=(--compress "${compress}")
args+=(--blocksize "${blocksize}")
args+=(--max-memory "${max_memory}")
args+=(--output output)
[[ "${overwrite}" == "true" ]] && args+=(--overwrite)

worker_script="${root_dir}/src/ingest_cmems.py"
echo "Executing: python ${worker_script} ${args[@]}"
"${ENV_PYTHON}" "${worker_script}" "${args[@]}"
