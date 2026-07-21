#!/usr/bin/env bash
# Frozon Sentinel-1 GRD ingest worker runner.
# Reads job parameters from _job.json and invokes src/ingest_s1grd.py
# for one acquisition date.
set -eo pipefail
set +u

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running Frozon Sentinel-1 GRD ingest worker..."

echo "=== Runtime conda discovery ==="
echo "ls /opt/conda/envs/:"; ls -la /opt/conda/envs/ 2>&1 || echo "  /opt/conda/envs not found"
echo "ls /opt/conda/envs/ingest/bin/ (top 5):"
{ ls /opt/conda/envs/ingest/bin/ 2>&1 | head -5; } || true
echo "image build stamp:"
cat "${root_dir}/.maap/ingest-s1grd/.build-stamp" 2>/dev/null || echo "  (no .build-stamp — image is pre-stamp)"
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
export PATH=/opt/conda/envs/ingest/bin:${PATH}
echo "PROJ_DATA=${PROJ_DATA}"
echo "GDAL_DATA=${GDAL_DATA}"
echo "PATH (first 80 chars): ${PATH:0:80}"

if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

# Pre-declare all vars.
input_https_urls=""
input_source="" cdse_secret_name=""
cmr_short_names="" cmr_temporal_start="" cmr_temporal_end="" cmr_bbox=""
filter_pattern=""
polarization="HH"
mosaic_date=""
earthdata_token_secret_name=""
calibrations="sigma0"
collection_id_template=""
collection_id="" s3_bucket="" s3_prefix="" role_arn=""
compress="DEFLATE" blocksize="512" max_memory="4096"
resampling="nearest" overview_resampling="average" overwrite="false"

eval "$(/opt/conda/envs/ingest/bin/python "${root_dir}/.maap/_lib/load_job_params.py" _job.json)"
filter_pattern="${filter:-${filter_pattern}}"
input_source="${input_source:-asf}"

echo "=== Parsed parameters ==="
echo "mosaic_date:               ${mosaic_date}"
echo "polarization:              ${polarization}"
echo "input_source:              ${input_source}"
echo "cdse_secret_name:          ${cdse_secret_name}"
echo "input_https_urls (len):    ${#input_https_urls}"
echo "cmr_short_names:           ${cmr_short_names}"
echo "cmr_temporal:              ${cmr_temporal_start} → ${cmr_temporal_end}"
echo "cmr_bbox:                  ${cmr_bbox}"
echo "filter:                    ${filter_pattern}"
echo "collection_id:             ${collection_id}"
echo "s3_bucket:                 ${s3_bucket}"
echo "s3_prefix:                 ${s3_prefix}"
echo "compress / block / max:    ${compress} / ${blocksize} / ${max_memory}"
echo "========================="

args=()
if [[ -n "${input_https_urls}" ]]; then
    args+=(--input-https-urls "${input_https_urls}")
elif [[ -n "${cmr_short_names}" ]]; then
    # cmr_short_names is a comma-separated list; emit each as a repeat of --cmr-short-name
    IFS=',' read -ra _sns <<< "${cmr_short_names}"
    for sn in "${_sns[@]}"; do
        args+=(--cmr-short-name "${sn}")
    done
    [[ -n "${cmr_temporal_start}" ]] && args+=(--cmr-temporal-start "${cmr_temporal_start}")
    [[ -n "${cmr_temporal_end}" ]]   && args+=(--cmr-temporal-end "${cmr_temporal_end}")
    [[ -n "${cmr_bbox}" ]]           && args+=(--cmr-bbox="${cmr_bbox}")
    [[ -n "${filter_pattern}" ]]     && args+=(--filter "${filter_pattern}")
else
    echo "ERROR: must supply either input_https_urls OR cmr_short_names"
    exit 1
fi

args+=(--input-source "${input_source}")
[[ -n "${cdse_secret_name}" ]]      && args+=(--cdse-secret-name "${cdse_secret_name}")
args+=(--polarization "${polarization}")
[[ -n "${mosaic_date}" ]]           && args+=(--mosaic-date "${mosaic_date}")
[[ -n "${earthdata_token_secret_name}" ]] && args+=(--earthdata-token-secret-name "${earthdata_token_secret_name}")
[[ -n "${calibrations}" ]]          && args+=(--calibrations "${calibrations}")
[[ -n "${collection_id_template}" ]] && args+=(--collection-id-template "${collection_id_template}")
[[ -n "${collection_id}" ]]         && args+=(--collection-id "${collection_id}")
[[ -n "${s3_bucket}" ]]             && args+=(--s3-bucket "${s3_bucket}")
[[ -n "${s3_prefix}" ]]             && args+=(--s3-prefix "${s3_prefix}")
[[ -n "${role_arn}" ]]              && args+=(--role-arn "${role_arn}")

args+=(--compress "${compress}")
args+=(--blocksize "${blocksize}")
args+=(--max-memory "${max_memory}")
args+=(--resampling "${resampling}")
args+=(--overview-resampling "${overview_resampling}")
args+=(--output output)
[[ "${overwrite}" == "true" ]] && args+=(--overwrite)

worker_script="${root_dir}/src/ingest_s1grd.py"
echo "Executing: python ${worker_script} ${args[@]}"
"${ENV_PYTHON}" "${worker_script}" "${args[@]}"
