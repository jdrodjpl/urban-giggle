#!/usr/bin/env bash
# Frozon ISS COG ingest worker runner.
# Reads job parameters from _job.json and invokes src/ingest_cog.py for one TIFF.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running Frozon ISS COG ingest worker..."

source activate ingest

if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

input_s3=$(jq -r '.params.input_s3 // empty' _job.json)
collection_id=$(jq -r '.params.collection_id // empty' _job.json)
s3_bucket=$(jq -r '.params.s3_bucket // empty' _job.json)
s3_prefix=$(jq -r '.params.s3_prefix // ""' _job.json)
role_arn=$(jq -r '.params.role_arn // empty' _job.json)
compress=$(jq -r '.params.compress // "DEFLATE"' _job.json)
blocksize=$(jq -r '.params.blocksize // "512"' _job.json)
max_memory=$(jq -r '.params.max_memory // "512"' _job.json)
resampling=$(jq -r '.params.resampling // "nearest"' _job.json)
overview_resampling=$(jq -r '.params.overview_resampling // "average"' _job.json)
overwrite=$(jq -r '.params.overwrite // "false"' _job.json)

# Fallback: input file staged via MAAP file parameter into ./input/
input_tiff=""
if [[ -z "${input_s3}" ]] && [ -d "input" ] && [ "$(ls -A input 2>/dev/null)" ]; then
    input_tiff=$(ls input/* | head -n 1)
    echo "Using staged input file: ${input_tiff}"
fi

echo "=== Parsed parameters ==="
echo "input_s3:      ${input_s3}"
echo "input_tiff:    ${input_tiff}"
echo "collection_id: ${collection_id}"
echo "s3_bucket:     ${s3_bucket}"
echo "s3_prefix:     ${s3_prefix}"
echo "compress:      ${compress}"
echo "blocksize:     ${blocksize}"
echo "max_memory:    ${max_memory}"
echo "========================="

args=()
if [[ -n "${input_s3}" ]]; then
    args+=(--input-s3 "${input_s3}")
elif [[ -n "${input_tiff}" ]]; then
    args+=(--input-tiff "${input_tiff}")
else
    echo "ERROR: no input provided (input_s3 or staged input/ file)"
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

worker_script="${root_dir}/src/ingest_cog.py"
echo "Executing: python ${worker_script} ${args[@]}"
conda run -n ingest --live-stream python "${worker_script}" "${args[@]}"
