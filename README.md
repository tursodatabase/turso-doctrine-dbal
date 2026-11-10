> [!IMPORTANT]
> **Binary parameters** — since v1.4.0, `ParameterType::BINARY` round-trips
> arbitrary bytes through a `libsqlblob:` marker transport (see [CHANGELOG](CHANGELOG.md)).
> Strings with invalid UTF-8 bound as `ParameterType::STRING` still store NULL
> (extension limitation); hex literals `x'…'` always store a real BLOB.

<p align="center">
  <a href="https://discord.gg/9eAaMnJtfY">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://i.imgur.com/UhuW3zm.png">
      <source media="(prefers-color-scheme: light)" srcset="https://i.imgur.com/vljWbfr.png">
      <img alt="Shows a black logo in light color mode and a white one in dark color mode." src="https://i.imgur.com/vGCC0I4.png">
    </picture>
  </a>
</p>

<p align="center">
  <img alt="Turso + Doctrine DBAL" src="https://i.imgur.com/r009n2u.png" width="1000">
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/github/license/tursodatabase/turso-doctrine-dbal?color=F05340" alt="MIT License" /></a>
  <a href="https://tur.so/discord-php"><img src="https://img.shields.io/discord/933071162680958986?color=F05340" alt="Discord" /></a>
  <a href="#contributors"><img src="https://img.shields.io/github/contributors/tursodatabase/turso-doctrine-dbal?color=F05340" alt="Contributors" /></a>
  <a href="https://packagist.org/packages/tursodatabase/turso-doctrine-dbal"><img src="https://img.shields.io/packagist/dt/tursodatabase/turso-doctrine-dbal?color=F05340" alt="Total downloads" /></a>
  <a href="https://github.com/tursodatabase/turso-doctrine-dbal/tags"><img src="https://img.shields.io/github/v/tag/tursodatabase/turso-doctrine-dbal?label=version&color=F05340" alt="Version Tag" /></a>
  <a href="https://github.com/tursodatabase/turso-doctrine-dbal/actions/workflows/tests.yml"><img src="https://github.com/tursodatabase/turso-doctrine-dbal/actions/workflows/tests.yml/badge.svg" alt="Testing" /></a>
</p>

<p align="center">
  SQLite for Production. Powered by <a href="https://turso.tech/libsql">libSQL</a> and the <a href="https://github.com/tursodatabase/turso-client-php/">libSQL PHP extension</a>.
</p>

<p align="center">
  libSQL is an open-contribution fork of SQLite. This package provides a
  Doctrine DBAL driver built on the native libSQL PHP extension; the
  <code>docker/symfony-proof/</code> integration harness and the test suite
  exercise migrations, ORM round trips and transactions end to end.
</p>

---

## Requirements

| Component | Requirement |
|---|---|
| PHP | 8.2, 8.3, 8.4 or 8.5 |
| doctrine/dbal | `^4.0` — CI tests 4.0.2, 4.5.1 and 5.0.x-dev |
| libSQL PHP extension | `1.7.0` or later |
| Turso credentials (remote/replica modes) | token + sync URL — see below |

> `Doctrine\DBAL\Result::getColumnName()` (used by schema introspection) requires dbal **4.1.0+**.

## Installation

```bash
composer require tursodatabase/turso-doctrine-dbal
```

Install the libSQL PHP extension — one of two ways:

**Laravel projects** — via the Artisan installer:

```bash
composer require darkterminal/turso-php-installer
php artisan turso-php:install
```

**Any stack** — download the tarball for your platform from
[turso-client-php releases](https://github.com/tursodatabase/turso-client-php/releases),
then enable it in `php.ini`:

```ini
extension=liblibsql_php.so     ; Linux (.dll on Windows)
```

Verify:

```bash
php -m | grep libsql
php -r 'echo LibSQL::version(), PHP_EOL;'
```

## Configuration

Connection parameters feed straight into Doctrine's `DriverManager::getConnection([...])`:

```php
<?php

use Doctrine\DBAL\DriverManager;
use Turso\Doctrine\DBAL\Driver;

$params = [
    'url' => ':memory:',                 // ':memory:', a filename, or omit sync settings
    'driverClass' => Driver::class,
];

// Local file
// $params = ['url' => 'database.sqlite', 'driverClass' => Driver::class];

// Remote (Turso Cloud)
// $params = [
//     'url' => ':memory:',              // or a local filename for embedded replica
//     'auth_token' => $_ENV['TURSO_AUTH_TOKEN'],
//     'sync_url' => $_ENV['TURSO_SYNC_URL'],
//     'driverClass' => Driver::class,
// ];

// Remote replica (embedded replica)
// $params = [
//     'url' => 'database.sqlite',       // local replica file
//     'auth_token' => $_ENV['TURSO_AUTH_TOKEN'],
//     'sync_url' => $_ENV['TURSO_SYNC_URL'],
//     'sync_interval' => 5,             // optional, default 5 (seconds)
//     'read_your_writes' => true,        // optional, default true
//     'driverClass' => Driver::class,
// ];

$conn = DriverManager::getConnection($params);
```

Get credentials with `turso db show --url <db-name>` and
`turso db tokens create <db-name>`, or from the
[Turso dashboard](https://dashboard.turso.io). Never commit tokens — read them from the environment.

## Connection types

| Conditions | Mode |
|---|---|
| `url` ends in `.db`/`.sqlite` with no `sync_url` | `local` |
| `url` ends in `.db`/`.sqlite` with `sync_url` + `auth_token` | `remote_replica` |
| `sync_url` without a local file | `remote` |
| everything else | `:memory:` |

> Writing `sqlite://path/to.db` is **not** required and won't behave as a file URL.
> Use a plain filename — the driver checks for `.db`/`.sqlite` substrings.
> On Symfony, put a plain filename in `DATABASE_URL` (not a `sqlite://` DSN) and set `driver_class` in the bundle config; since 1.3.1 the driver falls back to `dbname` automatically.

## Usage

Standard Doctrine DBAL APIs — Query Builder SQL, ORM, migrations, transactions,
streaming fetch iteration — work through the native driver:

```php
$conn = DriverManager::getConnection([
    'url' => ':memory:',
    'driverClass' => \Turso\Doctrine\DBAL\Driver::class,
]);

$conn->executeStatement('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)');
$conn->executeStatement('INSERT INTO users (name, age) VALUES (?, ?)', ['Budi Dalton', 49]);

$rows = $conn->executeQuery('SELECT * FROM users ORDER BY id')->fetchAllAssociative();
print_r($rows);
```

Transactions: `beginTransaction()` / `commit()` / `rollBack()`; streaming:
iterate `$stmt->fetchAssociative()` in a `while` loop; positional params are
numbered from 1; named params use `:name`; writes **throw** instead of
returning `false`; `lastInsertId()` returns a numeric string.

For a full working tour — fresh Symfony app, ORM round trip, transaction
rollback/commit, `doctrine:migrations:migrate` through the driver — see the
[Symfony proof harness](docker/symfony-proof/) and
[example/index.php](example/index.php).

## Documentation

- [CHANGELOG](CHANGELOG.md) — what changed, including the 1.3.1 Symfony
  persistence fix, binary support in 1.4.0, and the DBAL 4 compliance rework
- [Symfony proof harness](docker/symfony-proof/README.md) — how to reproduce
  every claim on this page from a clean checkout
- [Turso Cloud proof harness](docker/cloud-proof/README.md) — same for the
  Turso Cloud offline-writes flavor against a live cloud database
- [Turso documentation](https://docs.turso.tech/sdk/php/orm/doctrine-dbal) —
  official quickstart, examples and API reference for this package
- [turso-client-php](https://github.com/tursodatabase/turso-client-php) — the
  libSQL PHP extension source of truth
- [open issues](https://github.com/tursodatabase/turso-doctrine-dbal/issues) —
  known limitations are tracked in the open

## Offline-write modes

libSQL can apply writes to a local journal and push them to the server later —
either journaling through a local embedded replica to Turso Cloud (**journaled
flavor**), or against a self-hosted `sqld` (**sqld flavor**). Both need the
libSQL extension ≥ 1.7.0 and are wired through two connection flags
(`sqld_offline_mode` and `offline_writes`), mirroring the Laravel driver.

### Turso Cloud flavor

Embedded replica against Turso Cloud, journaled writes, manual `sync()` to push:

```php
$params = [
    'url' => 'database.sqlite',          // local replica file
    'auth_token' => $_ENV['TURSO_AUTH_TOKEN'],
    'sync_url' => $_ENV['TURSO_SYNC_URL'],
    'sync_interval' => 0,                // required for offline mode — manual sync only
    'read_your_writes' => true,
    'driverClass' => \Turso\Doctrine\DBAL\Driver::class,
    'offline_writes' => true,            // sqld_offline_mode stays false
];
```

Writes go to the local file until `sync()` pushes them. The server never sees
unsynced writes — verified against a live Turso Cloud database
(`TURSO_CLOUD_DB_URL` / `TURSO_CLOUD_DB_TOKEN`-gated proof in
[`tests/Feature/TursoCloudOfflineWriteTest.php`](tests/Feature/TursoCloudOfflineWriteTest.php)).

### Self-hosted sqld flavor

An embedded replica journaled against a local `sqld`, or against a `sqld`
server you run yourself:

```php
$params = [
    'url' => 'database.sqlite',
    'auth_token' => $_ENV['TURSO_AUTH_TOKEN'],
    'sync_url' => 'http://127.0.0.1:8080',
    'sync_interval' => 0,
    'read_your_writes' => true,
    'driverClass' => \Turso\Doctrine\DBAL\Driver::class,
    'sqld_offline_mode' => true,
    'offline_writes' => true,
];
```

Writes journal locally; `sync()` drains the journal to the server. Pending
operations and connectivity are inspectable:

```php
$conn->getNativeConnection()->isOnline();                  // bool
$conn->getNativeConnection()->getPendingOperationsCount(); // int
$conn->getNativeConnection()->sync();                      // push journaled rows
```

Writes against a reachable server go straight through; with the server
unreachable, they journal locally until the next `sync()`.

**Bootstrap requirement (Turso Cloud):** call `sync()` once on a fresh replica
file **before** any write — otherwise the push fails with a frame conflict.

Both flavors are covered by env-gated tests:

- `OfflineWriteTest` (sqld): outage → journal → reconnect → drain cycle
- `TursoCloudOfflineWriteTest` (cloud): journaled writes pushed via `sync()`
  and verified server-side, cleanup verified

## Testing

```bash
composer install
./vendor/bin/phpunit
```

Server-dependent tests are skipped by default. Remote/replica tests need a
seeded `sqld` on `8081`/`8082` and run under `TURSO_SQLD_AVAILABLE=true`
(seeded in CI). The Turso Cloud flavor is skipped separately and never touches
the cloud unless `TURSO_CLOUD_DB_URL` / `TURSO_CLOUD_DB_TOKEN` are set.

## Changelog

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed
recently.

## Contributing

Please see [open issues](https://github.com/tursodatabase/turso-doctrine-dbal/issues)
and the proof harness. Changes should pass CI and the Symfony proof with tests
for any behavior change. The house style is: prove it first, then document it.

## Security Vulnerabilities

Please do not open public issues for vulnerabilities. Report them privately via
[GitHub Security Advisories](https://github.com/tursodatabase/turso-doctrine-dbal/security/advisories/new)
and we will coordinate a fix.

## Contributors

![Contributors](https://contrib.nn.ci/api?no_bot=true&repo=tursodatabase/turso-doctrine-dbal)

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
