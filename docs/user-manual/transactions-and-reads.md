---
title: Transactions and reads
description: Transaction lifecycle, read behavior inside transactions, and result semantics.
section: Guide
eyebrow: Transactions
---

## Transaction lifecycle

```php
$conn->beginTransaction();
try {
    $conn->executeStatement("INSERT INTO users (name) VALUES ('Ada')");
    $conn->commit();
} catch (\Throwable $e) {
    $conn->rollBack();
    throw $e;
}
```

The driver wraps libSQL transaction objects. `beginTransaction()` creates a transaction, `commit()` commits, `rollBack()` rolls back, and internal state is reset.

## Reads inside transactions

Reads executed while a transaction is active go through the transaction executor/query path. This ensures transactional visibility for reads after writes.

```php
$conn->beginTransaction();
$conn->executeStatement("INSERT INTO users (name) VALUES ('Ada')");
$count = $conn->executeQuery('SELECT count(*) FROM users')->fetchOne();
$conn->commit();
```

## Result types

The driver returns `Turso\Doctrine\DBAL\Result` for read and write operations.

- Read query: wraps `LibSQLResult` or transactional envelope array.
- Write query: returns result with affected row count.

Available fetch methods follow DBAL `Result` contract: `fetchNumeric`, `fetchAssociative`, `fetchOne`, `fetchAllNumeric`, `fetchAllAssociative`, `fetchFirstColumn`.

## Freeing results

```php
$stmt = $conn->executeQuery('SELECT * FROM users');
$stmt->free();
```

`free()` finalizes/resets native result when possible and clears buffered rows.

## Column metadata

`getColumnName(int $index)` is available for read results. For Doctrine DBAL < 4.1.0, `Doctrine\DBAL\Result::getColumnName()` is not available; the package test skips accordingly.