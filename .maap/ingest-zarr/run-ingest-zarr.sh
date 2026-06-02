#!/usr/bin/env bash
# Frozon ISS Zarr ingest worker runner.
# Reads job parameters from _job.json and invokes src/ingest_zarr.py
# for the entire batch of new TIFF inputs at once.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running Frozon ISS Zarr ingest worker..."

echo "=== Runtime conda discovery ==="
echo "ls /opt/conda/envs/:"; ls -la /opt/conda/envs/ 2>&1 || true
echo "================================"
if [[ ! -x /opt/conda/envs/ingest/bin/python ]]; then
    echo "FATAL: /opt/conda/envs/ingest/bin/python not found." >&2
    exit 1
fi
ENV_PYTHON=/opt/conda/envs/ingest/bin/python

if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

input_s3_prefix=$(jq -r '.params.input_s3_prefix // empty' _job.json)
input_https_urls=$(jq -r '.params.input_https_urls // empty' _job.json)
earthdata_token_secret_name=$(jq -r '.params.earthdata_token_secret_name // empty' _job.json)
retain_days=$(jq -r '.params.retain_days // "0"' _job.json)
collection_id=$(jq -r '.params.collection_id // empty' _job.json)
s3_bucket=$(jq -r '.params.s3_bucket // empty' _job.json)
s3_prefix=$(jq -r '.params.s3_prefix // ""' _job.json)
role_arn=$(jq -r '.params.role_arn // empty' _job.json)
time_regex=$(jq -r '.params.time_regex // empty' _job.json)
chunk_size=$(jq -r '.params.chunk_size // "1024"' _job.json)
filter_pattern=$(jq -r '.params.filter // empty' _job.json)
exclude_pattern=$(jq -r '.params.exclude // empty' _job.json)
limit=$(jq -r '.params.limit // empty' _job.json)
allow_bounds_expansion=$(jq -r '.params.allow_bounds_expansion // "true"' _job.json)

for var in input_s3_prefix input_https_urls earthdata_token_secret_name \
           role_arn s3_prefix time_regex filter_pattern exclude_pattern limit; do
    val_lc=$(echo "${!var}" | tr '[:upper:]' '[:lower:]')
    if [[ "${val_lc}" == "none" || "${val_lc}" == "null" ]]; then
        eval "${var}=\"\""
    fi
done

echo "=== Parsed parameters ==="
echo "input_s3_prefix:          ${input_s3_prefix}"
echo "collection_id:            ${collection_id}"
echo "s3_bucket:                ${s3_bucket}"
echo "s3_prefix:                ${s3_prefix}"
echo "time_regex:               ${time_regex}"
echo "chunk_size:               ${chunk_size}"
echo "allow_bounds_expansion:   ${allow_bounds_expansion}"
echo "========================="

args=()
if [[ -n "${input_https_urls}" ]]; then
    args+=(--input-https-urls "${input_https_urls}")
    if [[ -n "${earthdata_token_secret_name}" ]]; then
        args+=(--earthdata-token-secret-name "${earthdata_token_secret_name}")
    fi
elif [[ -n "${input_s3_prefix}" ]]; then
    args+=(--input-s3-prefix "${input_s3_prefix}")
elif [ -d "input" ] && [ "$(ls -A input 2>/dev/null)" ]; then
    echo "Falling back to staged input/ directory"
    args+=(--input-tiff-dir "input")
else
    echo "ERROR: no input provided (input_https_urls, input_s3_prefix, or staged input/)"
    exit 1
fi
args+=(--retain-days "${retain_days}")

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
args+=(--output output)

worker_script="${root_dir}/src/ingest_zarr.py"
echo "Executing: python ${worker_script} ${args[@]}"
"${ENV_PYTHON}" "${worker_script}" "${args[@]}"
