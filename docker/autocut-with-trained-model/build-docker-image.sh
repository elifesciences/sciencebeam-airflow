#!/bin/bash

set -e

AUTOCUT_MODEL_URL=${1:-$AUTOCUT_MODEL_URL}
SOURCE_AUTOCUT_IMAGE=${2:-$SOURCE_AUTOCUT_IMAGE}
OUTPUT_AUTOCUT_IMAGE=${3:-$OUTPUT_AUTOCUT_IMAGE}
GCP_PROJECT=${4:-$GCP_PROJECT}

if [ -z "${AUTOCUT_MODEL_URL}" ]; then
    echo "Error: AUTOCUT_MODEL_URL required"
    exit 1
fi

if [ -z "${SOURCE_AUTOCUT_IMAGE}" ]; then
    echo "Error: SOURCE_AUTOCUT_IMAGE required"
    exit 1
fi

if [ -z "${OUTPUT_AUTOCUT_IMAGE}" ]; then
    echo "Error: OUTPUT_AUTOCUT_IMAGE required"
    exit 1
fi

SCRIPT_HOME="$(dirname "$0")"

echo "AUTOCUT_MODEL_URL=${AUTOCUT_MODEL_URL}"
echo "SOURCE_AUTOCUT_IMAGE=${SOURCE_AUTOCUT_IMAGE}"
echo "OUTPUT_AUTOCUT_IMAGE=${OUTPUT_AUTOCUT_IMAGE}"
echo "GCP_PROJECT=${GCP_PROJECT}"
echo "SCRIPT_HOME=${SCRIPT_HOME}"

temp_build_dir=/tmp/autocut-trained-model-build
echo "temp_build_dir=${temp_build_dir}"
rm -rf "${temp_build_dir}"
mkdir -p "${temp_build_dir}"
cp "${SCRIPT_HOME}"/* "${temp_build_dir}"

MODEL_FILENAME="model.pkl"
OUTPUT_MODEL_FILENAME="${temp_build_dir}/${MODEL_FILENAME}"

echo "copying ${AUTOCUT_MODEL_URL} to ${OUTPUT_MODEL_FILENAME}"
gsutil cp "${AUTOCUT_MODEL_URL}" "${OUTPUT_MODEL_FILENAME}"

if [ -z "${GCP_PROJECT}" ]; then
    echo "build local image: $OUTPUT_AUTOCUT_IMAGE"
    docker build \
        --build-arg "base_image=${SOURCE_AUTOCUT_IMAGE}" \
        --tag ${OUTPUT_AUTOCUT_IMAGE} \
        "${temp_build_dir}"
else
    echo "build image using gcloud build: $OUTPUT_AUTOCUT_IMAGE"
    gcloud builds submit --project "${GCP_PROJECT}" \
        --config "${SCRIPT_HOME}/config.yaml" \
        --substitutions "_BASE_IMAGE=${SOURCE_AUTOCUT_IMAGE},_IMAGE=${OUTPUT_AUTOCUT_IMAGE}" \
        "${temp_build_dir}"
fi
