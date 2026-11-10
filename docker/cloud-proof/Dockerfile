# Turso Doctrine DBAL — Turso Cloud offline-writes proof.
#
# Fresh Docker sandbox, fresh proof only. The harness boots a minimal
# CLI PHP application with the libSQL extension installed, then exercises
# the Turso Cloud journaled-writes flavor end to end:
#
#   bootstrap sync() -> DDL+INSERT journal locally -> sync() pushes ->
#   remote read-back -> DROP pushed -> cloud left clean.
#
# Build context must be the repository ROOT (the driver tree under review).
#   docker build -f docker/cloud-proof/Dockerfile .
#
# Build args:
#   DRIVER_SOURCE   "path" (default) or "packagist"
#   DRIVER_VERSION  Version constraint for the driver. Default "*".
#   LIBSQL_VERSION  libSQL extension version. Default "1.7.0".

FROM php:8.3-cli-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    COMPOSER_ALLOW_SUPERUSER=1 \
    COMPOSER_NO_INTERACTION=1

RUN apt-get update && apt-get install -q -y --no-install-recommends \
        git \
        curl \
        unzip \
        ca-certificates \
        libzip-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

ARG LIBSQL_VERSION=1.7.0
RUN set -eu; \
    mkdir -p /tmp/libsql-ext; \
    url="https://github.com/tursodatabase/turso-client-php/releases/download/turso-php-extension-v${LIBSQL_VERSION}/libsql_php-turso-php-extension-v${LIBSQL_VERSION}-php-8.3-nts-x86_64-unknown-linux-gnu.tar.gz"; \
    curl -fsSL -o /tmp/libsql-ext/libsql.tar.gz "$url"; \
    tar -xzf /tmp/libsql-ext/libsql.tar.gz -C /tmp/libsql-ext; \
    SO_PATH="$(find /tmp/libsql-ext -name 'liblibsql_php.so' -print -quit)"; \
    EXT_DIR="$(php -r 'echo ini_get("extension_dir");')"; \
    cp "$SO_PATH" "$EXT_DIR/liblibsql_php.so"; \
    echo 'extension=liblibsql_php.so' > /usr/local/etc/php/conf.d/70-libsql.ini; \
    php -m | tr 'A-Z' 'a-z' | grep libsql

WORKDIR /app

COPY . /driver-src

ARG DRIVER_SOURCE=path
ARG DRIVER_VERSION=*

# Minimal app: a single-file proof, install driver as a path repository.
RUN composer init --no-interaction --name turso/proof --description "Turso Cloud offline-writes proof" && \
    if [ "$DRIVER_SOURCE" = "path" ]; then \
        composer config repositories.turso-proof '{"type":"path","url":"/driver-src","options":{"symlink":false}}'; \
    fi && \
    composer require "tursodatabase/turso-doctrine-dbal:${DRIVER_VERSION}" --no-progress

COPY docker/cloud-proof/app/proof.php /app/proof.php

CMD ["php", "/app/proof.php"]
