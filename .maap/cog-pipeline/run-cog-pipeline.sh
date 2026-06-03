#!/usr/bin/env bash
# Frozon ISS COG Pipeline — orchestrator runner.
# Reads job parameters from _job.json and invokes src/pipeline_cog.py.
set -eo pipefail   # NOTE: dropped -u because we use ${var} freely with
                   # vars that may not be set in _job.json. The Python
                   # helper only emits keys actually present in the JSON;
                   # everything else stays unset and reads as empty.

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running Frozon ISS COG pipeline..."

if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

# Load all _job.json params as shell vars via the python helper.
# We use python instead of jq because jq is a system binary that
# wasn't reliably installed on MAAP's CI; python is always there.
# The helper also normalizes "none"/"null" → "" so the Python entry
# point doesn't see a literal --flag none.
eval "$(python3 "${root_dir}/.maap/_lib/load_job_params.py" _job.json)"

# Apply defaults for unset fields.
: "${s3_prefix:=}"
: "${maap_host:=api.maap-project.org}"
: "${compress:=DEFLATE}"
: "${blocksize:=512}"
: "${max_memory:=512}"
: "${resampling:=nearest}"
: "${overview_resampling:=average}"
: "${overwrite:=false}"
: "${upsert:=true}"
: "${local_download_path:=output}"
: "${input_source_type:=s3}"
: "${cmr_prefer_https:=true}"
: "${retain_days:=0}"
: "${scp_port:=22}"

default_queue=$(python3 -c "import json; d=json.load(open('_job.json')); print(d.get('job_info',{}).get('job_queue') or '')")
: "${job_queue:=${default_queue}}"

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
if [[ -n "${time_regex}" ]]; then
    args+=(--time-regex "${time_regex}")
fi
args+=(--retain-days "${retain_days}")
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
if [[ -n "${scp_host}" ]]; then
    args+=(--scp-host "${scp_host}")
    args+=(--scp-port "${scp_port}")
    if [[ -n "${scp_user}" ]]; then
        args+=(--scp-user "${scp_user}")
    fi
    if [[ -n "${scp_remote_dir}" ]]; then
        args+=(--scp-remote-dir "${scp_remote_dir}")
    fi
    if [[ -n "${scp_key_secret_name}" ]]; then
        args+=(--scp-key-secret-name "${scp_key_secret_name}")
    fi
fi

pipeline_script="${root_dir}/src/pipeline_cog.py"
echo "Executing: python3 ${pipeline_script} ${args[@]}"
python3 "${pipeline_script}" "${args[@]}"
