---
title: Offline writes
description: Configure sqld and Turso Cloud offline-write flavors, sync journals, and understand caveats.
section: Guide
eyebrow: Offline writes
---

## Overview

Offline writes allow local journaling of writes when connectivity is unavailable or deferred. The driver exposes two opt-in flavors, both requiring libSQL extension features (1.7.0+ recommended).

## sqld offline flavor

Use when running against sqld/libSQL server with offline mode support:

```php
$conn = DriverManager::getConnection([
    'url' => 'replica.db',
    'auth_token' => 'token',
    'sync_url' => 'http://127.0.0.1:8080',
    'sync_interval' => 0,
    'read_your_writes' => true,
    'driverClass' => Driver::class,
    'sqld_offline_mode' => true,
    'offline_writes' => true,
]);
```

Behavior:

- writes journal locally when offline;
- local reads can see journaled rows;
- `sync()` drains pending operations when reachable;
- `getPendingOperationsCount()` reports journal backlog.

## Turso Cloud offline-write flavor

Use with Turso Cloud embedded replica:

```php
$conn = DriverManager::getConnection([
    'url' => 'replica.db',
    'auth_token' => getenv('TURSO_CLOUD_DB_TOKEN'),
    'sync_url' => getenv('TURSO_CLOUD_DB_URL'),
    'sync_interval' => 0,
    'read_your_writes' => true,
    'driverClass' => Driver::class,
    'sqld_offline_mode' => false,
    'offline_writes' => true,
]);
```

Important: bootstrap with `sync()` before first writes to pull remote state.

```php
$conn->getNativeConnection()->sync(); // bootstrap pull
$conn->executeStatement('INSERT INTO ...');
$conn->getNativeConnection()->sync(); // push journal
```

## Sync API

```php
$native = $conn->getNativeConnection();
$native->sync();
$isOnline = $native->isOnline();
$pending = $native->getPendingOperationsCount();
```

## Testing and proofs

- `tests/Feature/OfflineWriteTest.php` — sqld/offline behavior, env-gated by `TURSO_SQLD_AVAILABLE=true`.
- `tests/Feature/TursoCloudOfflineWriteTest.php` — Turso Cloud flavor, env-gated by `TURSO_CLOUD_DB_URL` and `TURSO_CLOUD_DB_TOKEN`.
- Docker harness: `docker/cloud-proof/`.

## Caveats

- Offline writes are not a replacement for conflict-free distributed design; understand your write model.
- Always call bootstrap `sync()` for Turso Cloud flavor before first write.
- Treat tokens and remote URLs as secrets; do not log them.
- Clean up proof/test tables and local replica files.