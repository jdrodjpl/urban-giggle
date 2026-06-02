#!/usr/bin/env bash
# Frozon ISS COG ingest worker runner.
# Reads job parameters from _job.json and invokes src/ingest_cog.py for one TIFF.
set -euo pipefail

basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running Frozon ISS COG ingest worker..."

if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

input_s3=$(jq -r '.params.input_s3 // empty' _job.json)
input_https=$(jq -r '.params.input_https // empty' _job.json)
earthdata_token_secret_name=$(jq -r '.params.earthdata_token_secret_name // empty' _job.json)
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
scp_host=$(jq -r '.params.scp_host // empty' _job.json)
scp_port=$(jq -r '.params.scp_port // "22"' _job.json)
scp_user=$(jq -r '.params.scp_user // empty' _job.json)
scp_remote_dir=$(jq -r '.params.scp_remote_dir // empty' _job.json)
scp_key_secret_name=$(jq -r '.params.scp_key_secret_name // empty' _job.json)

# MAAP fills unset positional inputs with the YAML default "none";
# normalize so the Python entry point doesn't see --flag none.
for var in input_s3 input_https earthdata_token_secret_name role_arn s3_prefix \
           scp_host scp_user scp_remote_dir scp_key_secret_name; do
    val_lc=$(echo "${!var}" | tr '[:upper:]' '[:lower:]')
    if [[ "${val_lc}" == "none" || "${val_lc}" == "null" ]]; then
        eval "${var}=\"\""
    fi
done

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
if [[ -n "${input_s3}" ]]; then
    args+=(--input-s3 "${input_s3}")
elif [[ -n "${input_https}" ]]; then
    args+=(--input-https "${input_https}")
    if [[ -n "${earthdata_token_secret_name}" ]]; then
        args+=(--earthdata-token-secret-name "${earthdata_token_secret_name}")
    fi
elif [[ -n "${input_tiff}" ]]; then
    args+=(--input-tiff "${input_tiff}")
else
    echo "ERROR: no input provided (input_s3, input_https, or staged input/ file)"
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

# Find a proj.db rasterio can read. Prefer the conda base image's
# system-wide PROJ (newest, well-maintained) over pyproj's bundled one
# (sometimes ships an older proj.db that fails rasterio's version check).
unset PROJ_DATA PROJ_LIB
for candidate in /opt/conda/share/proj /usr/share/proj \
                 "$(python3 -c 'import pyproj; print(pyproj.datadir.get_data_dir())' 2>/dev/null)"; do
    if [[ -n "${candidate}" && -f "${candidate}/proj.db" ]]; then
        export PROJ_DATA="${candidate}"
        export PROJ_LIB="${candidate}"
        echo "PROJ_DATA=${PROJ_DATA}"
        break
    fi
done

echo "Executing: python3 ${worker_script} ${args[@]}"
python3 "${worker_script}" "${args[@]}"
