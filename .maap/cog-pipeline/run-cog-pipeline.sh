#!/usr/bin/env bash
# Frozon ISS COG Pipeline — orchestrator runner.
# Reads job parameters from _job.json and invokes src/pipeline_cog.py.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running Frozon ISS COG pipeline..."

source activate ingest

if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

input_s3=$(jq -r '.params.input_s3 // empty' _job.json)
input_s3_prefix=$(jq -r '.params.input_s3_prefix // empty' _job.json)
collection_id=$(jq -r '.params.collection_id // empty' _job.json)
s3_bucket=$(jq -r '.params.s3_bucket // empty' _job.json)
s3_prefix=$(jq -r '.params.s3_prefix // ""' _job.json)
role_arn=$(jq -r '.params.role_arn // empty' _job.json)
cmss_logger_host=$(jq -r '.params.cmss_logger_host // empty' _job.json)
mmgis_host=$(jq -r '.params.mmgis_host // empty' _job.json)
titiler_token_secret_name=$(jq -r '.params.titiler_token_secret_name // empty' _job.json)
maap_host=$(jq -r '.params.maap_host // "api.maap-project.org"' _job.json)
compress=$(jq -r '.params.compress // "DEFLATE"' _job.json)
blocksize=$(jq -r '.params.blocksize // "512"' _job.json)
max_memory=$(jq -r '.params.max_memory // "512"' _job.json)
resampling=$(jq -r '.params.resampling // "nearest"' _job.json)
overview_resampling=$(jq -r '.params.overview_resampling // "average"' _job.json)
overwrite=$(jq -r '.params.overwrite // "false"' _job.json)
upsert=$(jq -r '.params.upsert // "true"' _job.json)
post_stac_webhook_url=$(jq -r '.params.post_stac_webhook_url // empty' _job.json)
post_stac_webhook_token_secret_name=$(jq -r '.params.post_stac_webhook_token_secret_name // empty' _job.json)
filter_pattern=$(jq -r '.params.filter // empty' _job.json)
limit=$(jq -r '.params.limit // empty' _job.json)
local_download_path=$(jq -r '.params.local_download_path // "output"' _job.json)
input_source_type=$(jq -r '.params.input_source_type // "s3"' _job.json)
cmr_short_name=$(jq -r '.params.cmr_short_name // empty' _job.json)
cmr_version=$(jq -r '.params.cmr_version // empty' _job.json)
cmr_temporal_start=$(jq -r '.params.cmr_temporal_start // empty' _job.json)
cmr_temporal_end=$(jq -r '.params.cmr_temporal_end // empty' _job.json)
cmr_bbox=$(jq -r '.params.cmr_bbox // empty' _job.json)
cmr_granule_ids=$(jq -r '.params.cmr_granule_ids // empty' _job.json)
cmr_prefer_https=$(jq -r '.params.cmr_prefer_https // "true"' _job.json)
earthdata_token_secret_name=$(jq -r '.params.earthdata_token_secret_name // empty' _job.json)

# MAAP fills unset positional inputs with the YAML's `default:` literal,
# which for optional fields is the string "none". Normalize that to empty
# so the Python entry point doesn't see a literal --flag none.
for var in input_s3 input_s3_prefix role_arn s3_prefix \
           cmss_logger_host mmgis_host titiler_token_secret_name \
           post_stac_webhook_url post_stac_webhook_token_secret_name \
           filter_pattern limit \
           cmr_short_name cmr_version cmr_temporal_start cmr_temporal_end \
           cmr_bbox cmr_granule_ids earthdata_token_secret_name; do
    val_lc=$(echo "${!var}" | tr '[:upper:]' '[:lower:]')
    if [[ "${val_lc}" == "none" || "${val_lc}" == "null" ]]; then
        eval "${var}=\"\""
    fi
done

default_queue=$(jq -r '.job_info.job_queue // empty' _job.json)
job_queue=$(jq -r '.params.job_queue // empty' _job.json)
if [[ -z "${job_queue}" ]]; then
    job_queue="${default_queue}"
fi

echo "=== Parsed parameters ==="
echo "input_s3:        ${input_s3}"
echo "input_s3_prefix: ${input_s3_prefix}"
echo "collection_id:   ${collection_id}"
echo "s3_bucket:       ${s3_bucket}"
echo "s3_prefix:       ${s3_prefix}"
echo "job_queue:       ${job_queue}"
echo "compress:        ${compress}"
echo "blocksize:       ${blocksize}"
echo "max_memory:      ${max_memory}"
echo "========================="

args=()
if [[ -n "${input_s3}" ]]; then
    args+=(--input-s3 "${input_s3}")
fi
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

args+=(--compress "${compress}")
args+=(--blocksize "${blocksize}")
args+=(--max-memory "${max_memory}")
args+=(--resampling "${resampling}")
args+=(--overview-resampling "${overview_resampling}")

if [[ "${overwrite}" == "true" ]]; then
    args+=(--overwrite)
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
if [[ -n "${filter_pattern}" ]]; then
    args+=(--filter "${filter_pattern}")
fi
if [[ -n "${limit}" ]]; then
    args+=(--limit "${limit}")
fi
if [[ -n "${local_download_path}" && "${local_download_path}" != "output" ]]; then
    args+=(--local-download-path "${local_download_path}")
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
    args+=(--cmr-bbox "${cmr_bbox}")
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

pipeline_script="${root_dir}/src/pipeline_cog.py"
echo "Executing: python ${pipeline_script} ${args[@]}"
conda run -n ingest --live-stream python "${pipeline_script}" "${args[@]}"
