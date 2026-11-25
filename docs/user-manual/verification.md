---
title: Verification and proof
description: Run PHPUnit tests and Docker proof harnesses for Symfony and Turso Cloud scenarios.
section: Operations
eyebrow: Verification
---

## PHPUnit

```bash
composer install
./vendor/bin/phpunit
```

The test suite includes unit and feature tests. Some feature tests are environment-gated.

## Environment-gated tests

| Test | Env vars | Purpose |
|---|---|---|
| `OfflineWriteTest` | `TURSO_SQLD_AVAILABLE=true` | sqld/offline-write behavior. |
| `RemoteReplicaTest` | `TURSO_SQLD_AVAILABLE=true` | remote and embedded replica smoke. |
| `TursoCloudOfflineWriteTest` | `TURSO_CLOUD_DB_URL`, `TURSO_CLOUD_DB_TOKEN` | real Turso Cloud offline-write proof. |

## Symfony proof harness

`docker/symfony-proof/` builds a fresh Symfony app in Docker, installs the driver, runs migrations, and executes a proof command.

```bash
docker/symfony-proof/proof.sh
```

It proves DBAL query, ORM persist/find, DQL, raw insert, transaction rollback/commit, streaming fetch, named/positional params, last insert id, and server version.

## Turso Cloud proof harness

`docker/cloud-proof/` verifies the Turso Cloud offline-write flavor against a real Turso Cloud database.

```bash
TURSO_CLOUD_DB_URL=libsql://<db>-<org>.turso.io \
TURSO_CLOUD_DB_TOKEN=<token> \
docker/cloud-proof/proof.sh
```

It proves local journaling, sync push, remote read-back, and cleanup.

## CI

GitHub Actions workflow: `.github/workflows/tests.yml`. Run local PHPUnit before pushing docs/code changes. For docs-only PRs, still build Jekyll and sanity-check pages.