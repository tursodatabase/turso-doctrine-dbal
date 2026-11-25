---
title: Getting started
description: Install the libSQL PHP extension, add the package, and create your first Doctrine DBAL connection.
section: Introduction
eyebrow: Quick start
---

## Install the libSQL PHP extension

This driver requires the native `libsql_php` extension (recommended 1.7.0+).

### Laravel installer

```bash
composer require darkterminal/turso-php-installer
php artisan turso-php:install
```

### Manual install

Download the release tarball for your platform from [turso-client-php releases](https://github.com/tursodatabase/turso-client-php/releases), then enable it in `php.ini`:

```ini
extension=liblibsql_php.so
```

Verify:

```bash
php -m | grep libsql
php -r 'echo LibSQL::version(), PHP_EOL;'
```

## Install the package

```bash
composer require tursodatabase/turso-doctrine-dbal
```

## First connection

```php
<?php

use Doctrine\DBAL\DriverManager;
use Turso\Doctrine\DBAL\Driver;

$conn = DriverManager::getConnection([
    'url' => ':memory:',
    'driverClass' => Driver::class,
]);

$conn->executeStatement('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)');
$conn->executeStatement('INSERT INTO notes (body) VALUES (?)', ['hello']);
echo $conn->executeQuery('SELECT body FROM notes')->fetchOne();
```

## Symfony/Doctrine integration

Use the same `driverClass` in Doctrine DBAL configuration. For Symfony, keep DBAL config explicit and pass `driverClass` as `Turso\Doctrine\DBAL\Driver::class`. The package README and Wiki contain framework-specific notes.

## Next steps

- Configure connection modes in [Configuration](configuration.md).
- Understand mode selection in [Connection modes](connection-modes.md).
- Read [Known limitations](known-limitations.md) before production use.