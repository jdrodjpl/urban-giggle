#!/usr/bin/env bash
# Frozon ISS Zarr Pipeline — orchestrator runner.
# Reads job parameters from _job.json and invokes src/pipeline_zarr.py.
set -eo pipefail
set +u   # explicitly disable nounset (see cog-pipeline/run-cog-pipeline.sh)

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running Frozon ISS Zarr pipeline..."

if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

# Load all _job.json params as shell vars via the python helper
# (replaces jq, which wasn't reliably installed on MAAP's CI).
eval "$(python3 "${root_dir}/.maap/_lib/load_job_params.py" _job.json)"

: "${s3_prefix:=}"
: "${maap_host:=api.maap-project.org}"
: "${chunk_size:=1024}"
: "${allow_bounds_expansion:=true}"
: "${upsert:=true}"
: "${input_source_type:=s3}"
: "${cmr_prefer_https:=true}"
: "${retain_days:=0}"

default_queue=$(python3 -c "import json; d=json.load(open('_job.json')); print(d.get('job_info',{}).get('job_queue') or '')")
: "${job_queue:=${default_queue}}"

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

args+=(--input-source-type "${input_source_type}")
if [[ -n "${cmr_short_name}" ]]; then
    args+=(--cmr-short-name "${cmr_short_name}")
fi
if [[ -n "${cmr_version}" ]]; then
    args+=(--cmr-version "${cmr_version}")
fi
if [[ -n "${cmr_temporal_start}" ]]; then
    args+=(--cmr-temporal-start "${cmr_temporal_start}")
fi
if [[ -n "${cmr_temporal_end}" ]]; then
    args+=(--cmr-temporal-end "${cmr_temporal_end}")
fi
if [[ -n "${cmr_bbox}" ]]; then
    # Use --flag=value to keep argparse from misreading a leading '-' as another flag.
    args+=(--cmr-bbox="${cmr_bbox}")
fi
if [[ -n "${cmr_granule_ids}" ]]; then
    args+=(--cmr-granule-ids "${cmr_granule_ids}")
fi
if [[ "${cmr_prefer_https}" == "true" ]]; then
    args+=(--cmr-prefer-https)
else
    args+=(--no-cmr-prefer-https)
fi
if [[ -n "${earthdata_token_secret_name}" ]]; then
    args+=(--earthdata-token-secret-name "${earthdata_token_secret_name}")
fi
args+=(--retain-days "${retain_days}")

pipeline_script="${root_dir}/src/pipeline_zarr.py"
echo "Executing: python3 ${pipeline_script} ${args[@]}"
python3 "${pipeline_script}" "${args[@]}"
