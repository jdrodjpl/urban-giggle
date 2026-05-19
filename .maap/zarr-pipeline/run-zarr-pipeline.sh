#!/usr/bin/env bash
# Frozon ISS Zarr Pipeline — orchestrator runner.
# Reads job parameters from _job.json and invokes src/pipeline_zarr.py.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running Frozon ISS Zarr pipeline..."

source activate ingest

if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

input_s3_prefix=$(jq -r '.params.input_s3_prefix // empty' _job.json)
collection_id=$(jq -r '.params.collection_id // empty' _job.json)
s3_bucket=$(jq -r '.params.s3_bucket // empty' _job.json)
s3_prefix=$(jq -r '.params.s3_prefix // ""' _job.json)
role_arn=$(jq -r '.params.role_arn // empty' _job.json)
cmss_logger_host=$(jq -r '.params.cmss_logger_host // empty' _job.json)
mmgis_host=$(jq -r '.params.mmgis_host // empty' _job.json)
titiler_token_secret_name=$(jq -r '.params.titiler_token_secret_name // empty' _job.json)
maap_host=$(jq -r '.params.maap_host // "api.maap-project.org"' _job.json)
time_regex=$(jq -r '.params.time_regex // empty' _job.json)
chunk_size=$(jq -r '.params.chunk_size // "1024"' _job.json)
filter_pattern=$(jq -r '.params.filter // empty' _job.json)
exclude_pattern=$(jq -r '.params.exclude // empty' _job.json)
limit=$(jq -r '.params.limit // empty' _job.json)
allow_bounds_expansion=$(jq -r '.params.allow_bounds_expansion // "true"' _job.json)
upsert=$(jq -r '.params.upsert // "true"' _job.json)
post_stac_webhook_url=$(jq -r '.params.post_stac_webhook_url // empty' _job.json)
post_stac_webhook_token_secret_name=$(jq -r '.params.post_stac_webhook_token_secret_name // empty' _job.json)

default_queue=$(jq -r '.job_info.job_queue // empty' _job.json)
job_queue=$(jq -r '.params.job_queue // empty' _job.json)
if [[ -z "${job_queue}" ]]; then
    job_queue="${default_queue}"
fi

echo "=== Parsed parameters ==="
echo "input_s3_prefix:        ${input_s3_prefix}"
echo "collection_id:          ${collection_id}"
echo "s3_bucket:              ${s3_bucket}"
echo "s3_prefix:              ${s3_prefix}"
echo "job_queue:              ${job_queue}"
echo "time_regex:             ${time_regex}"
echo "chunk_size:             ${chunk_size}"
echo "allow_bounds_expansion: ${allow_bounds_expansion}"
echo "upsert:                 ${upsert}"
echo "========================="

args=()
if [[ -n "${input_s3_prefix}" ]]; then
    args+=(--input-s3-prefix "${input_s3_prefix}")
fi
if [[ -n "${collection_id}" ]]; then
    args+=(--collection-id "${collection_id}")
fi
if [[ -n "${s3_bucket}" ]]; then
    args+=(--s3-bucket "${s3_bucket}")
fi
if [[ -n "${s3_prefix}" ]]; then
    args+=(--s3-prefix "${s3_prefix}")
fi
if [[ -n "${role_arn}" ]]; then
    args+=(--role-arn "${role_arn}")
fi
if [[ -n "${cmss_logger_host}" ]]; then
    args+=(--cmss-logger-host "${cmss_logger_host}")
fi
if [[ -n "${mmgis_host}" ]]; then
    args+=(--mmgis-host "${mmgis_host}")
fi
if [[ -n "${titiler_token_secret_name}" ]]; then
    args+=(--titiler-token-secret-name "${titiler_token_secret_name}")
fi
if [[ -n "${maap_host}" && "${maap_host}" != "api.maap-project.org" ]]; then
    args+=(--maap-host "${maap_host}")
fi
if [[ -n "${job_queue}" ]]; then
    args+=(--job-queue "${job_queue}")
fi
if [[ -n "${time_regex}" ]]; then
    args+=(--time-regex "${time_regex}")
fi
args+=(--chunk-size "${chunk_size}")
if [[ -n "${filter_pattern}" ]]; then
    args+=(--filter "${filter_pattern}")
fi
if [[ -n "${exclude_pattern}" ]]; then
    args+=(--exclude "${exclude_pattern}")
fi
if [[ -n "${limit}" ]]; then
    args+=(--limit "${limit}")
fi
if [[ "${allow_bounds_expansion}" == "true" ]]; then
    args+=(--allow-bounds-expansion)
else
    args+=(--no-allow-bounds-expansion)
fi
if [[ "${upsert}" == "true" ]]; then
    args+=(--upsert)
else
    args+=(--no-upsert)
fi
if [[ -n "${post_stac_webhook_url}" ]]; then
    args+=(--post-stac-webhook-url "${post_stac_webhook_url}")
fi
if [[ -n "${post_stac_webhook_token_secret_name}" ]]; then
    args+=(--post-stac-webhook-token-secret-name "${post_stac_webhook_token_secret_name}")
fi

pipeline_script="${root_dir}/src/pipeline_zarr.py"
echo "Executing: python ${pipeline_script} ${args[@]}"
conda run -n ingest --live-stream python "${pipeline_script}" "${args[@]}"
