---
title: Configuration
description: DriverManager parameters, mode selection inputs, and offline-write flags.
section: Guide
eyebrow: Configuration
---

## Basic DriverManager params

```php
use Doctrine\DBAL\DriverManager;
use Turso\Doctrine\DBAL\Driver;

$conn = DriverManager::getConnection([
    'url' => 'database.sqlite',
    'driverClass' => Driver::class,
]);
```

## Supported params

| Param | Type | Purpose |
|---|---|---|
| `url` | string | `:memory:`, local `.db/.sqlite`, or used with sync settings. |
| `driverClass` | class-string | Must be `Turso\Doctrine\DBAL\Driver::class`. |
| `auth_token` | string | Turso/libSQL auth token for remote/replica modes. |
| `sync_url` | string | Remote libSQL/Turso endpoint. |
| `sync_interval` | int | Replica sync interval seconds (default 5 in replica mode). |
| `read_your_writes` | bool | Replica read-your-writes (default true in replica mode). |
| `encryption_key` | string | Local encryption key when supported by extension. |
| `sqld_offline_mode` | bool | Enable sqld offline flavor (with `offline_writes`). |
| `offline_writes` | bool | Enable offline writes. |

## Driver options / framework mode

The driver also inspects `driverOptions`/`options` for framework integration. When `use_framework=true`, it can read `url`, `auth_token`, `sync_url`, path, or memory flags from framework-shaped config.

## Connection mode selection

Mode is resolved by `Driver`:

| Condition | Mode |
|---|---|
| local `.db/.sqlite` URL and `sync_url` present | `remote_replica` |
| `sync_url` present without local file | `remote` |
| local `.db/.sqlite` URL without `sync_url` | `local` |
| otherwise | `memory` |

The driver normalizes `file:` URLs and also checks `dbname` because Doctrine DBAL may consume `url` during DSN parsing.

## Remote replica defaults

When in `remote_replica` mode, defaults are merged:

```php
[
    'sync_interval' => 5,
    'read_your_writes' => true,
    'encryption_key' => '',
]
```

## Offline write flags

Both flags are opt-in:

- sqld flavor: `sqld_offline_mode=true` and `offline_writes=true`
- Turso Cloud flavor: `offline_writes=true` (with `sqld_offline_mode=false`/unset)

See [Offline writes](offline-writes.md).