---
title: Connection modes
description: Memory, local file, remote, and embedded replica connection modes.
section: Guide
eyebrow: Modes
---

## Memory

```php
$conn = DriverManager::getConnection([
    'url' => ':memory:',
    'driverClass' => Driver::class,
]);
```

Use for tests and ephemeral scripts. Data is lost when connection closes.

## Local file

```php
$conn = DriverManager::getConnection([
    'url' => 'database.sqlite',
    'driverClass' => Driver::class,
]);
```

Use for local persistence. Any `.db` or `.sqlite` path without `sync_url` resolves to local mode.

## Remote

```php
$conn = DriverManager::getConnection([
    'url' => ':memory:',
    'auth_token' => getenv('TURSO_AUTH_TOKEN'),
    'sync_url' => getenv('TURSO_SYNC_URL'),
    'driverClass' => Driver::class,
]);
```

Remote mode connects directly to the libSQL/Turso endpoint. No local replica file is required.

## Embedded replica

```php
$conn = DriverManager::getConnection([
    'url' => 'database.sqlite',
    'auth_token' => getenv('TURSO_AUTH_TOKEN'),
    'sync_url' => getenv('TURSO_SYNC_URL'),
    'sync_interval' => 5,
    'read_your_writes' => true,
    'driverClass' => Driver::class,
]);
```

Embedded replica keeps a local file and syncs with remote. Good for read latency and hybrid local/remote workflows.

## Mode detection caveats

- Doctrine DBAL may consume `url` and pass `dbname`; the driver checks both.
- `file:` and `file://` prefixes are normalized.
- If `sync_url` is present with a local file, mode becomes `remote_replica`.
- If no local file and no sync URL, mode falls back to `memory`.

## Sync and offline helpers

The DBAL connection wrapper exposes native helpers:

```php
$native = $conn->getNativeConnection();
$native->sync();
$native->isOnline();
$native->getPendingOperationsCount();
```

These are most relevant for replica/offline modes; behavior in plain online modes is extension-defined.