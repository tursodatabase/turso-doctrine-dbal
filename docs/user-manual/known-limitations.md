---
title: Known limitations
description: Extension and driver limitations to account for when using Turso Doctrine DBAL.
section: Reference
eyebrow: Limitations
---

## Binary parameter transport

The libSQL PHP extension has no typed blob parameter binding. Strings bind as TEXT; invalid UTF-8 strings may arrive as NULL. The driver transports `ParameterType::BINARY` values as a `libsqlblob:` marker with double-base64 payload.

Implication: binary round-trip works when using `ParameterType::BINARY` and driver codec-aware reads. Do not assume arbitrary binary strings bound as plain TEXT are safe.

## getColumnName() DBAL version

`Doctrine\DBAL\Result::getColumnName()` requires Doctrine DBAL >= 4.1.0. The driver implements `getColumnName()` on its result, but DBAL's API surface depends on your DBAL version.

## Read query detection

The driver treats statements starting with `SELECT`, `PRAGMA`, `EXPLAIN`, or `WITH` (case-insensitive, ignoring leading whitespace) as read queries. Unusual statements that return rows but do not match this pattern may need explicit handling.

## Offline mode behavior

Offline write behavior depends on libSQL extension version and mode. Some operations that are no-ops in online mode can behave differently in offline/journaled modes. Test your exact mode with the proof harnesses.

## Server-dependent features

Remote, replica, and Turso Cloud offline-write tests require real services/credentials and are env-gated. CI/local success in memory mode does not prove remote network behavior.

## DSN/URL handling

Doctrine DBAL can consume `url` before driver receives params. The driver compensates by checking `dbname`, but framework-specific DBAL config can still be tricky; keep config explicit and test connection mode.