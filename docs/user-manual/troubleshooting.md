---
title: Troubleshooting
description: Common installation, connection, and runtime issues for Turso Doctrine DBAL.
section: Reference
eyebrow: Troubleshooting
---

## `libsql` extension not found

Symptoms: class `LibSQL` not found, extension missing, or driver fails at connect.

Fix:

```bash
php -m | grep libsql
php -r 'echo LibSQL::version(), PHP_EOL;'
```

Install the correct libSQL PHP extension build for your PHP version, OS, and architecture. Ensure CLI and web SAPIs load the same `php.ini`/scan dir.

## Remote/replica connects to wrong mode

Check params:

- local file + no `sync_url` => local
- `sync_url` + no local file => remote
- local file + `sync_url` => remote_replica
- no match => memory

Also check `dbname` when using Doctrine DBAL DSN parsing; driver falls back to it.

## Turso Cloud offline writes fail before first write

Bootstrap the replica before journaling writes:

```php
$conn->getNativeConnection()->sync();
```

Turso Cloud offline-write flavor needs initial pull; skipping it can cause frame conflicts.

## Binary data corrupted or NULL

Bind binary data as `ParameterType::BINARY`:

```php
$conn->executeStatement('INSERT INTO t (data) VALUES (?)', [$bytes], [ParameterType::BINARY]);
```

Plain TEXT binding for invalid UTF-8 is an extension limitation.

## Transaction read returns unexpected type

If you hit unexpected transactional read behavior, verify extension version and run the package tests. The driver supports both `LibSQLResult` and transactional envelope array reads.

## Doctrine DBAL version mismatch

The package requires `doctrine/dbal:^4.0`. CI tests selected 4.x/5.x-dev matrix cases per repo config; pin and test the exact DBAL version used by your app.

## Local verification

```bash
composer install
./vendor/bin/phpunit
cd docs && /home/linuxbrew/.linuxbrew/lib/ruby/gems/3.4.0/bin/jekyll build --trace
```