#!/usr/bin/env bash
#
# Turso Cloud offline-writes proof — fresh Docker sandbox, real Turso Cloud
# database. Aborts if credentials are missing. Nothing is installed on your
# host beyond Docker.
#
#   TURSO_CLOUD_DB_URL=libsql://<db>-<org>.turso.io \
#   TURSO_CLOUD_DB_TOKEN=<token> \
#   docker/cloud-proof/proof.sh
#
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR/../.."

if [ -z "${TURSO_CLOUD_DB_URL:-}" ] || [ -z "${TURSO_CLOUD_DB_TOKEN:-}" ]; then
    echo "error: TURSO_CLOUD_DB_URL and TURSO_CLOUD_DB_TOKEN must be set." >&2
    echo "Get credentials: turso db show --url <db-name> && turso db tokens create <db-name>" >&2
    exit 2
fi

echo "==> Building proof image"
docker build -f docker/cloud-proof/Dockerfile -t turso-cloud-proof .

echo "==> Running cloud proof"
docker run --rm \
    -e TURSO_CLOUD_DB_URL="$TURSO_CLOUD_DB_URL" \
    -e TURSO_CLOUD_DB_TOKEN="$TURSO_CLOUD_DB_TOKEN" \
    turso-cloud-proof

echo "==> Wipe image with: docker rmi turso-cloud-proof"
