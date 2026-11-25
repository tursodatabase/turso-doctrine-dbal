---
title: API reference
description: Public classes and key methods provided by Turso Doctrine DBAL.
section: Reference
eyebrow: Reference
---

## Package namespace

`Turso\Doctrine\DBAL`

## Core classes

| Class | Role |
|---|---|
| `Driver` | Implements Doctrine DBAL driver. Detects connection mode, creates LibSQL connection, returns DBAL connection wrapper. |
| `Connection` | Implements Doctrine DBAL connection. Prepares statements, executes queries, manages transactions, quotes strings, exposes native LibSQL connection. |
| `Statement` | Implements DBAL statement. Binds values, encodes binary params, executes through connection. |
| `Result` | Implements DBAL result. Supports numeric/associative fetch, row/column counts, free, column names. |
| `BlobCodec` | Internal transport codec for binary values. |
| `Exception` | DBAL driver exception wrapper. |

## Driver

```php
use Turso\Doctrine\DBAL\Driver;

$params = ['url' => 'database.sqlite', 'driverClass' => Driver::class];
```

Key behavior:

- `connect(array $params): Connection`
- `getDatabasePlatform(ServerVersionProvider $versionProvider): SQLitePlatform`
- `getExceptionConverter(): ExceptionConverter`

Connection modes: `memory`, `local`, `remote`, `remote_replica`.

## Connection

Key methods:

- `prepare(string $sql): Statement`
- `query(string $sql): Result`
- `exec(string $sql): int|string`
- `executeStatement(string $sql, array $parameters): Result`
- `beginTransaction()`, `commit()`, `rollBack()`
- `lastInsertId(): int|string`
- `getNativeConnection(): LibSQL`
- `getServerVersion(): string`

## Statement

- `bindValue(int|string $param, mixed $value, ParameterType $type): void`
- `execute(): Result`

Binary values with `ParameterType::BINARY` are encoded via `BlobCodec::encode()`.

## Result

- `fetchNumeric()`, `fetchAssociative()`, `fetchOne()`
- `fetchAllNumeric()`, `fetchAllAssociative()`, `fetchFirstColumn()`
- `rowCount()`, `columnCount()`, `getColumnName(int $index)`
- `free()`

## Native LibSQL helpers

Through `$conn->getNativeConnection()` you can access extension capabilities such as `sync()`, `isOnline()`, and `getPendingOperationsCount()` where supported. These are especially relevant for replica/offline-write modes.