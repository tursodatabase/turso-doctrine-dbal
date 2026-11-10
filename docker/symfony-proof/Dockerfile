# Turso Doctrine DBAL — isolated proof of usage.
#
# Build context must be the repository ROOT (the driver tree under review).
#   docker build -f docker/symfony-proof/Dockerfile .
#
# The image builds a fresh Symfony 7.4 application, installs the driver the
# same way a user would, installs the libSQL PHP extension from the official
# release tarball, runs doctrine migrations, and executes proof:run — a
# console command that asserts the whole integration end to end.
#
# Build args:
#   DRIVER_SOURCE   "path" (default — install from this checkout) or
#                   "packagist" (install the published package instead)
#   DRIVER_VERSION  Version constraint for the driver. Default "*".
#   LIBSQL_VERSION  libSQL extension version to install. Default "1.7.0".

FROM php:8.3-cli-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    COMPOSER_ALLOW_SUPERUSER=1 \
    COMPOSER_NO_INTERACTION=1

RUN apt-get update && apt-get install -q -y --no-install-recommends \
        git \
        curl \
        unzip \
        ca-certificates \
        libsqlite3-dev \
        libzip-dev \
        libssl-dev \
        sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

# Install the libSQL PHP extension from the official release tarball
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

# Fresh Symfony 7.4 application + doctrine stack
RUN composer create-project symfony/skeleton:"^7.4" /app --prefer-dist --no-progress \
    && composer require doctrine/doctrine-bundle doctrine/orm doctrine/migrations doctrine/doctrine-migrations-bundle --no-progress

# Register App\Migrations\ PSR-4 (migrations/ is not autoloaded by the skeleton)
RUN php -r '$f = "composer.json"; $d = json_decode(file_get_contents($f)); if (!isset($d->{"autoload-dev"})) { $d->{"autoload-dev"} = new stdClass(); } if (!isset($d->{"autoload-dev"}->{"psr-4"})) { $d->{"autoload-dev"}->{"psr-4"} = new stdClass(); } $d->{"autoload-dev"}->{"psr-4"}->{"App\\Migrations\\"} = "migrations/"; file_put_contents($f, json_encode($d, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n");' \
    && composer dump-autoload

# The driver source tree under review
COPY . /driver-src

ARG DRIVER_SOURCE=path
ARG DRIVER_VERSION=*

# Install the driver the way a user would
RUN if [ "$DRIVER_SOURCE" = "path" ]; then \
        composer config repositories.turso-proof '{"type":"path","url":"/driver-src","options":{"symlink":false}}'; \
    fi \
    && composer require "tursodatabase/turso-doctrine-dbal:${DRIVER_VERSION}" --no-progress

# Recipe overlay: env + dbal config + entity + migration + console proof command
COPY docker/symfony-proof/app/.env /app/.env
COPY docker/symfony-proof/app/doctrine.yaml /app/config/packages/doctrine.yaml
COPY docker/symfony-proof/app/doctrine_migrations.yaml /app/config/packages/doctrine_migrations.yaml
COPY docker/symfony-proof/app/migrations /app/migrations
COPY docker/symfony-proof/app/Book.php /app/src/Entity/Book.php
COPY docker/symfony-proof/app/ProofCommand.php /app/src/Command/ProofCommand.php

RUN composer dump-autoload

CMD ["sh", "-c", "php bin/console doctrine:migrations:migrate --no-interaction && php bin/console proof:run"]
