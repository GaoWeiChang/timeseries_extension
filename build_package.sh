#!/bin/bash
set -e

IMAGE_NAME="timeseries-builder"
OUTPUT_FILE="timeseries-extension.tar.gz"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=========================================="
echo "  Timeseries Extension - Package Builder  "
echo "=========================================="


# Build image
echo ""
echo "[1/3] Building Docker image..."
docker build \
    --target builder \
    -f "${SCRIPT_DIR}/Dockerfile" \
    -t "${IMAGE_NAME}" \
    "${SCRIPT_DIR}"


# Extract artifacts
echo ""
echo "[2/3] Extracting built artifacts..."
CONTAINER_ID=$(docker create "${IMAGE_NAME}")
mkdir -p "${SCRIPT_DIR}/artifacts"

docker cp "${CONTAINER_ID}:/usr/pgsql-16/lib/simple_timeseries.so"                     "${SCRIPT_DIR}/artifacts/"
docker cp "${CONTAINER_ID}:/usr/pgsql-16/share/extension/simple_timeseries.control"    "${SCRIPT_DIR}/artifacts/"
docker cp "${CONTAINER_ID}:/usr/pgsql-16/share/extension/simple_timeseries--1.0.sql"   "${SCRIPT_DIR}/artifacts/"

docker rm "${CONTAINER_ID}"


# Compress into tar.gz
echo ""
echo "[3/3] Creating tar.gz package..."
cp "${SCRIPT_DIR}/install.sh"   "${SCRIPT_DIR}/artifacts/"
cp "${SCRIPT_DIR}/uninstall.sh" "${SCRIPT_DIR}/artifacts/"
chmod +x "${SCRIPT_DIR}/artifacts/install.sh" \
        "${SCRIPT_DIR}/artifacts/uninstall.sh"

tar -czf "${SCRIPT_DIR}/${OUTPUT_FILE}" -C "${SCRIPT_DIR}/artifacts" .

# Clean up
rm -rf "${SCRIPT_DIR}/artifacts"


echo ""
echo "Done! Package created: ${OUTPUT_FILE}"