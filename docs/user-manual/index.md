---
title: Overview
description: What Turso Doctrine DBAL provides, when to use it, and where to start.
section: Introduction
eyebrow: User manual
---

## What is this package?

**Turso Doctrine DBAL** (`tursodatabase/turso-doctrine-dbal`) is a Doctrine DBAL 4 driver for libSQL/Turso. It uses the native `libsql_php` extension and exposes standard Doctrine DBAL behavior: connections, prepared statements, transactions, query execution, and result fetching.

It supports multiple connection modes:

- in-memory (`:memory:`)
- local SQLite/libSQL file
- remote Turso/libSQL endpoint
- embedded replica with remote sync
- offline-write flavors (sqld and Turso Cloud)

## When to use it

Use this package when your PHP application already uses Doctrine DBAL or needs a familiar database abstraction over libSQL/Turso.

It fits well for:

- Symfony/Doctrine DBAL applications;
- standalone PHP services using DBAL;
- local development with SQLite-like files;
- edge/replica setups with remote sync;
- offline-capable workflows where writes can be journaled and synced.

## Quick example

```php
use Doctrine\DBAL\DriverManager;
use Turso\Doctrine\DBAL\Driver;

$conn = DriverManager::getConnection([
    'url' => 'database.sqlite',
    'driverClass' => Driver::class,
]);

$conn->executeStatement('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
$conn->executeStatement('INSERT INTO users (name) VALUES (?)', ['Ada']);
$name = $conn->executeQuery('SELECT name FROM users WHERE id = ?', [1])->fetchOne();
```

## Requirements

| Component | Version |
|---|---|
| PHP | 8.2, 8.3, 8.4, or 8.5 |
| Doctrine DBAL | ^4.0 |
| libSQL PHP extension | 1.7.0+ recommended |

## Documentation map

- [Getting started](getting-started.md) — install extension and package.
- [Configuration](configuration.md) — driver params and mode selection.
- [Connection modes](connection-modes.md) — memory, local, remote, replica.
- [Basic usage](basic-usage.md) — queries, parameters, fetches.
- [Transactions and reads](transactions-and-reads.md) — tx behavior and streaming.
- [Offline writes](offline-writes.md) — sqld and Turso Cloud flavors.
- [Verification and proof](verification.md) — tests and Docker proof harnesses.
- [Known limitations](known-limitations.md) and [Troubleshooting](troubleshooting.md).
- [API reference](reference/api.md) — driver classes and behavior.