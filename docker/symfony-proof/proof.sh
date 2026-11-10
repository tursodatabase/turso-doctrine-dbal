#!/usr/bin/env bash
#
# Isolated proof of usage for tursodatabase/turso-doctrine-dbal — a fresh
# Symfony 7.4 app, real doctrine migrations, ORM + DBAL round trip, printed
# as a single machine-parseable JSON line.
#
#   docker/symfony-proof/proof.sh
#
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR/../.."

echo "==> Building proof image"
docker build -f docker/symfony-proof/Dockerfile -t turso-symfony-proof .

echo "==> Running migrations + proof assertions"
docker run --rm turso-symfony-proof

echo "==> Done. Wipe image with: docker rmi turso-symfony-proof"
