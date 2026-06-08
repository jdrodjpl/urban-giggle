#!/usr/bin/env bash
# Frozon ISS COG ingest worker runner.
# Reads job parameters from _job.json and invokes src/ingest_cog.py for one TIFF.
set -eo pipefail
set +u

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running Frozon ISS COG ingest worker..."

echo "=== Runtime conda discovery ==="
echo "ls /opt/conda/envs/:"; ls -la /opt/conda/envs/ 2>&1 || echo "  /opt/conda/envs not found"
echo "ls /opt/conda/envs/ingest/bin/ (top 5):"
ls /opt/conda/envs/ingest/bin/ 2>&1 | head -5
echo "find any 'ingest' env on disk:"
{ find / -maxdepth 6 -type d -name ingest 2>/dev/null | head; } || true
echo "================================"

# Use the env's python directly — bypass conda activate.
if [[ ! -x /opt/conda/envs/ingest/bin/python ]]; then
    echo "FATAL: /opt/conda/envs/ingest/bin/python not found." >&2
    echo "This means the build's 'conda env create' didn't actually create the env, OR" >&2
    echo "the runtime conda lives somewhere other than /opt/conda." >&2
    exit 1
fi
ENV_PYTHON=/opt/conda/envs/ingest/bin/python
echo "Using ${ENV_PYTHON}"

if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

# Pre-declare all vars (in case shell inherits nounset).
input_s3="" input_https="" input_s3_urls="" input_https_urls="" mosaic_date=""
earthdata_token_secret_name=""
collection_id="" s3_bucket="" s3_prefix="" role_arn=""
compress="DEFLATE" blocksize="512" max_memory="512"
resampling="nearest" overview_resampling="average" overwrite="false"
scp_host="" scp_port="22" scp_user="" scp_remote_dir="" scp_key_secret_name=""

# Load all _job.json params via the python helper (jq isn't on PATH —
# it's inside /opt/conda/envs/ingest/bin/ which we don't activate).
eval "$(/opt/conda/envs/ingest/bin/python "${root_dir}/.maap/_lib/load_job_params.py" _job.json)"

# Fallback: input file staged via MAAP file parameter into ./input/
input_tiff=""
if [[ -z "${input_s3}" && -z "${input_https}" ]] && [ -d "input" ] && [ "$(ls -A input 2>/dev/null)" ]; then
    input_tiff=$(ls input/* | head -n 1)
    echo "Using staged input file: ${input_tiff}"
fi

echo "=== Parsed parameters ==="
echo "input_s3:      ${input_s3}"
echo "input_https:   ${input_https}"
echo "input_tiff:    ${input_tiff}"
echo "collection_id: ${collection_id}"
echo "s3_bucket:     ${s3_bucket}"
echo "s3_prefix:     ${s3_prefix}"
echo "compress:      ${compress}"
echo "blocksize:     ${blocksize}"
echo "max_memory:    ${max_memory}"
echo "========================="

args=()
if [[ -n "${input_https_urls}" ]]; then
    # Daily-mosaic mode (HTTPS+EDL): JSON list of URLs.
    args+=(--input-https-urls "${input_https_urls}")
    if [[ -n "${earthdata_token_secret_name}" ]]; then
        args+=(--earthdata-token-secret-name "${earthdata_token_secret_name}")
    fi
    if [[ -n "${mosaic_date}" ]]; then
        args+=(--mosaic-date "${mosaic_date}")
    fi
elif [[ -n "${input_s3_urls}" ]]; then
    # Daily-mosaic mode (S3): JSON list of URLs.
    args+=(--input-s3-urls "${input_s3_urls}")
    if [[ -n "${mosaic_date}" ]]; then
        args+=(--mosaic-date "${mosaic_date}")
    fi
elif [[ -n "${input_s3}" ]]; then
    args+=(--input-s3 "${input_s3}")
elif [[ -n "${input_https}" ]]; then
    args+=(--input-https "${input_https}")
    if [[ -n "${earthdata_token_secret_name}" ]]; then
        args+=(--earthdata-token-secret-name "${earthdata_token_secret_name}")
    fi
elif [[ -n "${input_tiff}" ]]; then
    args+=(--input-tiff "${input_tiff}")
else
    echo "ERROR: no input provided"
    exit 1
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

args+=(--compress "${compress}")
args+=(--blocksize "${blocksize}")
args+=(--max-memory "${max_memory}")
args+=(--resampling "${resampling}")
args+=(--overview-resampling "${overview_resampling}")
args+=(--output output)

if [[ "${overwrite}" == "true" ]]; then
    args+=(--overwrite)
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

worker_script="${root_dir}/src/ingest_cog.py"

echo "Executing: python ${worker_script} ${args[@]}"
"${ENV_PYTHON}" "${worker_script}" "${args[@]}"
