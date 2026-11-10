# Changelog

All notable changes to `tursodatabase/turso-doctrine-dbal` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.1] - 2026-11-11

- fix(driver): **silent data loss for Symfony/dbal users on `driverClass`** — dbal consumes `url` at connection-build time (the filename survives only as `dbname`), while the Symfony recipe defaults inject `url: ':memory:'`. The driver therefore always opened `:memory:`, migrations appeared to succeed, and nothing ever persisted. `resolveDatabaseUrl()` now prefers `url` when it names a real path, then `dbname`, synthesizing `:memory:` only as a last resort; `file:` prefixes are normalized. Verified end-to-end in a real Symfony 7.4 kernel (proof:run 10/10, `all_ok: true`)
- feat(ci): PHPUnit 11 suite — unit tests for connection-mode detection (incl. dbname fallback) and quoting, plus feature tests for DDL/DML, streaming fetch, transactions, transactional reads, `lastInsertId()`, and env-gated remote/replica integration against two seeded sqld on 8081/8082
- feat(ci): 10-leg GitHub Actions matrix — PHP 8.2/8.3/8.4/8.5 × doctrine/dbal 4.0.2, ^4.5, ^5.0@dev; libSQL extension installed from official release tarball; `_boot`-seeded sqld handles WAL bootstrap on both 8081/8082; composer audit and example smoke included
- feat(proof): `docker/symfony-proof/proof.sh` — builds a fresh Symfony 7.4 app from the tree under review, installs libSQL 1.7.0, runs migrations through the driver, and asserts 10 checks (dbal + ORM + DQL + transactions + streaming fetch + named/positional params + `lastInsertId()` + `getServerVersion()`) — all_ok: true
- fix(driver): `Driver` now implements `Doctrine\DBAL\Driver` directly instead of extending `AbstractSQLiteDriver`, which dbal 5.0 declares `readonly` (fatal incompatibility before this change); PHP 8.2 inheritance rules satisfied for every dbal major

## [1.3.0] - 2026-11-11

Burned in by two maintenance PRs (reviewed in PR #1):

- fix(driver): reads inside a transaction no longer crash — `LibSQLTransaction::query()` always returns the raw envelope array (never a `LibSQLResult`); `executeReadQuery()` required one and threw `RuntimeException: Expected LibSQLResult`. `Result` now adapts the envelope via `Result::forReadEnvelope()` (assoc rows feed `fetchAssociative()` directly; numeric rows are derived with `array_values()`); `columnCount()`, `getColumnName()`, `rowCount()` and `free()` are envelope-aware. `LibSQLResult` is still accepted for forward compatibility. Verified on libsql_php 1.7.0 with `SELECT count(*)` / `SELECT *` inside `beginTransaction()`. (Contributed as PR #2)
- feat(driver): DBAL 4 compliance rework (from PR #1, author @andersonpem) —
  - `Result` rewritten around a buffered, consuming iterator: fixes `fetchFirstColumn()` calling `array_map()` over a scalar (fatal) and multi-row fetch iteration (previous implementation never advanced past the first row)
  - `lastInsertId()` now returns the actual row id (via `LibSQL::lastInsertedId()`, present since libsql_php 1.7.0) instead of row-change counts
  - `exec()` and `rowCount()` now declare `int|string` per the DBAL 4 contract; `getColumnName(int $index): string` added for DBAL ≥ 4.1 schema introspection (`Doctrine\DBAL\Result::getColumnName()`)
  - `Statement::execute()` normalizes positional bindings (1-based → 0-based) and named parameters
  - remote/replica detection via `sync_url` is now explicit about what it accepts; `driverClass` parameters are read from `driverOptions`/`options` uniformly
  - transaction routing goes through `currentExecutor()` (`LibSQLTransaction` when active, otherwise the base connection)
- verified against doctrine/dbal 4.0.2 (locked) and 4.5.1, including remote and remote-replica modes against a real seeded sqld server, and `getColumnName()` inside a transaction on 4.5.1
- compat: **PHP requirement raised to `^8.2`** (PHP 8.1 is EOL; doctrine/dbal 4.5 already requires it)
- known limitation (pre-existing, tracked): binding raw binary strings via `ParameterType::BINARY` stores NULL — the extension accepts PHP strings as text only; store binaries base64-encoded or use a `TEXT` sentinel for now

## [1.2.1] - 2024-08-20

Reconstructed from git history (this project had no changelog until 1.3.0).

- refactor: `Result` reworked around a native `LibSQLIterator`
- docs: README refresh

## [1.2.0] - 2024-06-11

- refactor: fetch methods unified

## [1.1.0] - 2024-06-10

- refactor(driver): connection logic updated for consistency and readability
- feat(connection): transaction handling via `LibSQLTransaction`
- fix(result): `fetchOne()` corrected for standalone mode
- refactor(statement): parameter binding simplified

## [1.0.2] - 2024-06-07

- build: renamed vendor to `tursodatabase/turso-doctrine-dbal`
