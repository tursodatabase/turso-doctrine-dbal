# Turso Cloud Proof — Doctrine DBAL

Isolated, sandboxed proof of the **Turso Cloud offline-writes flavor** for
`tursodatabase/turso-doctrine-dbal`.

Fresh Docker container, real Turso Cloud database, no state shared with your
host or `docker/proof`/`docker/symfony-proof`. Verifies the journaled-writes
flavor end to end.

## Quick start

```bash
TURSO_CLOUD_DB_URL=libsql://<db>-<org>.turso.io \
TURSO_CLOUD_DB_TOKEN=<token> \
docker/cloud-proof/proof.sh
```

You get:

```
PROOF-CLOUD: {"ok":N,"fail":0,"all_ok":true,...}
```

## What it proves

| # | Check | What it proves |
|---|-------|----------------|
| 1 | `journal_writes_local` | DDL+INSERT journal locally on the embedded replica; server never sees unsynced writes |
| 2 | `remote_read_back` | `sync()` pushes the journal; a plain remote connection (independent of the replica) reads the rows |
| 3 | `remote_table_gone` | `DROP` through the replica + `sync()` push removes the table server-side |

Inside the harness:

1. opens an embedded-replica connection against Turso Cloud with
   `offline_writes: true, sqld_offline_mode: false, sync_interval: 0`
2. calls `sync()` once — **bootstrap pull before any write** (Turso Cloud
   requires it, otherwise the push fails with a frame conflict)
3. journals DDL+INSERT locally
4. pushes with `sync()`
5. reads back server-side via an independent remote connection
6. drops the table, pushes
7. cloud database verified clean after the run (random table name; cleanup
   is enforced in `finally`)

## What it does NOT prove

- The sqld offline flavor — see `tests/Feature/OfflineWriteTest.php` and
  `docker/proof/` in turso-driver-laravel for that flavor's counterpart
- Multi-client concurrency / branching semantics
- Encryption at rest through this connection flavor

## Build args

| Arg | Default | Purpose |
|-----|---------|---------|
| `LIBSQL_VERSION` | `1.7.0` | libSQL extension version to install |
| `DRIVER_SOURCE` | `path` | `path` = install from this checkout; `packagist` = published package |
| `DRIVER_VERSION` | `*` | version constraint for the driver |

## Local run without Docker

Requires PHP ≥ 8.2 with the libSQL extension loaded and the package
installed via `composer`. Then:

```bash
TURSO_CLOUD_DB_URL=libsql://... TURSO_CLOUD_DB_TOKEN=... php proof.php
```
