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

<p align="center">
  Full user manual & tutorial now lives on the
  <a href="https://github.com/tursodatabase/turso-doctrine-dbal/wiki">Wiki</a>.
</p>

---

## Requirements

| Component | Requirement |
|---|---|
| PHP | 8.2, 8.3, 8.4 or 8.5 |
| doctrine/dbal | `^4.0` — CI tests 4.0.2, 4.5.1 and 5.0.x-dev |
| libSQL PHP extension | `1.7.0` or later |
| Turso credentials (remote/replica modes) | token + sync URL — see the Wiki |

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
then enable it in `php.ini` (`extension=liblibsql_php.so`).

Verify:

```bash
php -m | grep libsql
php -r 'echo LibSQL::version(), PHP_EOL;'
```

## Configuration

```php
<?php

use Doctrine\DBAL\DriverManager;
use Turso\Doctrine\DBAL\Driver;

$params = [
    'url' => ':memory:',                 // ':memory:', a filename, or omit sync settings
    'driverClass' => Driver::class,
];

// Local file:
// $params = ['url' => 'database.sqlite', 'driverClass' => Driver::class];

// Remote (Turso Cloud):
// $params = [
//     'url' => ':memory:',
//     'auth_token' => $_ENV['TURSO_AUTH_TOKEN'],
//     'sync_url' => $_ENV['TURSO_SYNC_URL'],
//     'driverClass' => Driver::class,
// ];

// Remote replica (embedded replica):
// $params = [
//     'url' => 'database.sqlite',
//     'auth_token' => $_ENV['TURSO_AUTH_TOKEN'],
//     'sync_url' => $_ENV['TURSO_SYNC_URL'],
//     'sync_interval' => 5,             // optional, default 5 (seconds)
//     'read_your_writes' => true,
//     'driverClass' => Driver::class,
// ];

$conn = DriverManager::getConnection($params);
```

All options (defaults, connection-mode selection, Symfony bundle recipe,
`DATABASE_URL` handling, `getColumnName()` dbal 4.1 caveat) are documented on
the [Wiki · Configuration](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Configuration).

## Connection types

| Conditions | Mode |
|---|---|
| `url` ends in `.db`/`.sqlite` with no `sync_url` | `local` |
| `url` ends in `.db`/`.sqlite` with `sync_url` + `auth_token` | `remote_replica` |
| `sync_url` without a local file | `remote` |
| everything else | `:memory:` |

Worked examples for every mode: [Wiki · Connection Types](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Connection-Types).

## Usage

Standard DBAL APIs — `executeStatement`, `executeQuery`, transactions,
streaming fetch iteration, schema introspection — work through the native driver.

Transactions: `beginTransaction()` / `commit()` / `rollBack()`.
Streaming: iterate `$stmt->fetchAssociative()` in a `while` loop.
Named parameters use `:name`, positional are numbered from 1.
Writes **throw** instead of returning `false`.

Full tour: [Wiki · Basic Usage](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Basic-Usage).

## Offline-write modes

Two opt-in flavors (both need extension ≥ 1.7.0):

| Driver params | Value | Flavor |
|---|---|---|
| `offline_writes` | `true` | Turso Cloud (beta) — journaled push to the cloud |
| `sqld_offline_mode` + `offline_writes` | both `true` | Self-hosted sqld — journal + `sync()` drain |

Full guide with verified output: [Wiki · Offline Writes](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Offline-Writes).

## Documentation

The full user manual & tutorial lives on the **[Wiki](https://github.com/tursodatabase/turso-doctrine-dbal/wiki)**:

- [Installation](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Installation)
- [Configuration](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Configuration)
- [Connection Types](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Connection-Types)
- [Basic Usage](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Basic-Usage)
- [Offline Writes (two flavors)](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Offline-Writes)
- [Verification & Proof Harness](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Verification)
- [Known Limitations](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Known-Limitations)
- [Troubleshooting](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Troubleshooting)

Official quickstart/API reference for this package is at
[docs.turso.tech/sdk/php/orm/doctrine-dbal](https://docs.turso.tech/sdk/php/orm/doctrine-dbal).

Links for the verification story: [docker/symfony-proof](docker/symfony-proof/) (10-assert proof), [docker/cloud-proof](docker/cloud-proof/) (cloud flavor), [CHANGELOG](CHANGELOG.md) and the test suite (`./vendor/bin/phpunit`).

## Testing

```bash
composer install
./vendor/bin/phpunit
```

Server-dependent tests are skipped by default. The Turso Cloud flavor is
env-gated. See [Wiki · Verification](https://github.com/tursodatabase/turso-doctrine-dbal/wiki/Verification).

## Changelog

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed
recently.

## Contributing

Please see [open issues](https://github.com/tursodatabase/turso-doctrine-dbal/issues)
and the proof harnesses. Changes should pass CI and the Symfony proof with tests
for any behavior change. The house style is: prove it first, then document it.

## Security Vulnerabilities

Please do not open public issues for vulnerabilities. Report them privately via
[GitHub Security Advisories](https://github.com/tursodatabase/turso-doctrine-dbal/security/advisories/new)
and we will coordinate a fix.

## Contributors

![Contributors](https://contrib.nn.ci/api?no_bot=true&repo=tursodatabase/turso-doctrine-dbal)

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
