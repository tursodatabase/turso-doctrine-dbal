---
title: Basic usage
description: Execute DDL/DML, bind parameters, fetch rows, and use standard Doctrine DBAL APIs.
section: Guide
eyebrow: Usage
---

## Execute statements

```php
$conn->executeStatement('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
$affected = $conn->executeStatement('INSERT INTO users (name) VALUES (?)', ['Ada']);
```

`executeStatement()` returns affected rows/count for write statements.

## Query rows

```php
$rows = $conn->executeQuery('SELECT id, name FROM users ORDER BY id')->fetchAllAssociative();
$names = $conn->executeQuery('SELECT name FROM users ORDER BY id')->fetchFirstColumn();
$one = $conn->executeQuery('SELECT count(*) FROM users')->fetchOne();
```

## Named and positional parameters

```php
$conn->executeStatement('INSERT INTO users (name) VALUES (:name)', ['name' => 'Ada']);
$conn->executeStatement('INSERT INTO users (name) VALUES (?)', ['Grace']);
```

Positional DBAL params are bound as 1-based in DBAL API; the driver normalizes them for the extension.

## Streaming fetch

```php
$stmt = $conn->executeQuery('SELECT * FROM users ORDER BY id');
while ($row = $stmt->fetchAssociative()) {
    // process row
}
```

The result implementation buffers rows for safe iteration semantics; `fetchAssociative()` advances and returns `false` at end.

## Last insert ID

```php
$conn->executeStatement('INSERT INTO users (name) VALUES (?)', ['Ada']);
$id = $conn->lastInsertId();
```

Inside transactions, the driver reads `last_insert_rowid()` through the transaction query path.

## Binary parameters

```php
use Doctrine\DBAL\ParameterType;

$bytes = random_bytes(32);
$conn->executeStatement('INSERT INTO blobs (data) VALUES (?)', [$bytes], [ParameterType::BINARY]);
$back = $conn->executeQuery('SELECT data FROM blobs')->fetchOne();
```

Binary strings are encoded with `BlobCodec` marker transport because the extension lacks typed blob parameter binding.

## Schema and DBAL tooling

Because this is a DBAL driver, schema introspection and Doctrine schema tools can work through standard DBAL APIs where supported by the platform/driver behavior. Test with your specific DBAL/ORM workflows; the Symfony proof harness exercises migrations and ORM round trips.