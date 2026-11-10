<p align="center">
  <a href="https://github.com/tursodatabase/turso-doctrine-dbal">
    <img alt="Turso + Doctrine DBAL" src="https://i.imgur.com/r009n2u.png" width="1000">
  </a>
</p>

<h3 align="center">A native libSQL driver for Doctrine DBAL</h3>

<p align="center">
  SQLite-compatible embedded and remote databases for Doctrine-based PHP
  applications, powered by the <a href="https://turso.tech/libsql">libSQL</a>
  engine and the native <a href="https://github.com/tursodatabase/turso-client-php">libSQL PHP extension</a>.
</p>

---

`tursodatabase/turso-doctrine-dbal` implements Doctrine DBAL's driver contract
on top of the native libSQL extension — not on PDO. Standard DBAL
`executeQuery` / `executeStatement`, transactions, streaming fetch iteration,
and schema introspection (`getColumnName()`) all work through the
[`LibSQL`](src/) connection.

## Requirements

| Component | Requirement |
|---|---|
| PHP | 8.2, 8.3, 8.4 or 8.5 |
| doctrine/dbal | `^4.0` — CI tests 4.0.2, 4.5.1 and 5.0.x-dev |
| libSQL PHP extension | `1.7.0` or later |
| Turso credentials (remote/replica modes) | token + sync URL — see [Configuration](#configuration) |

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

Four connection modes. Example parameters (all feed straight into Doctrine's
`DriverManager::getConnection([...])`):

### In-memory

```php
$params = [
    'url' => ':memory:',
    'driverClass' => \Turso\Doctrine\DBAL\Driver::class,
];
```

### Local file

```php
$params = [
    'url' => 'database.sqlite',
    'driverClass' => \Turso\Doctrine\DBAL\Driver::class,
];
```

### Remote (Turso Cloud)

```php
$params = [
    'url' => ':memory:',          // or a local filename for embedded replica
    'auth_token' => $_ENV['TURSO_AUTH_TOKEN'],
    'sync_url' => $_ENV['TURSO_SYNC_URL'],   // e.g. libsql://your-db.turso.io
    'driverClass' => \Turso\Doctrine\DBAL\Driver::class,
];
```

### Remote replica (embedded replica)

```php
$params = [
    'url' => 'database.sqlite',   // local replica file
    'auth_token' => $_ENV['TURSO_AUTH_TOKEN'],
    'sync_url' => $_ENV['TURSO_SYNC_URL'],
    'sync_interval' => 5,          // optional, default 5 (seconds)
    'read_your_writes' => true,    // optional, default true
    'driverClass' => \Turso\Doctrine\DBAL\Driver::class,
];
```

> Get credentials: `turso db show --url <db-name>` and `turso db tokens create <db-name>`,
> or the [Turso dashboard](https://dashboard.turso.io). Never commit tokens — read them from the environment.

### Connection mode selection

| Conditions | Mode |
|---|---|
| `url` ends in `.db`/`.sqlite` with no `sync_url` | `local` |
| `url` ends in `.db`/`.sqlite` with `sync_url` + `auth_token` | `remote_replica` |
| `sync_url` without a local file | `remote` |
| everything else | `:memory:` |

> Note: Writing `sqlite://path/to.db` is **not** required and won't behave as a file URL.
> Use a plain filename — the driver checks for `.db`/`.sqlite` substrings.

## Usage

```php
<?php
require __DIR__ . '/vendor/autoload.php';

use Doctrine\DBAL\DriverManager;
use Turso\Doctrine\DBAL\Driver;

$conn = DriverManager::getConnection([
    'url' => ':memory:',
    'driverClass' => Driver::class,
]);

$conn->executeStatement('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)');
$conn->executeStatement('INSERT INTO users (name, age) VALUES (?, ?)', ['Budi Dalton', 49]);

$rows = $conn->executeQuery('SELECT * FROM users ORDER BY id')->fetchAllAssociative();
print_r($rows);
```

Conventions (compatible with all Doctrine DBAL APIs):

- **Prepared parameters are positional** (`?` bindings numbered from 1) or named
  (`:name`) — both work; don't mix them in the same statement
- **Transactions**: `beginTransaction()` / `commit()` / `rollBack()` (nested/
  savepoints are not supported — see Limitations)
- **Streaming**: iterate `$stmt->fetchAssociative()` in a `while` loop —
  `fetchAllAssociative()` also buffers lazily; nothing returns a row twice
- **Writes throw**: `executeStatement` / `executeQuery` throw on constraint
  violations instead of returning `false` silently
- **`lastInsertId()`** returns a numeric string for identity columns

## Symfony (doctrine-bundle)

```yaml
# config/packages/doctrine.yaml
doctrine:
    dbal:
        driver_class: Turso\Doctrine\DBAL\Driver
        url: '%env(resolve:DATABASE_URL)%'
```

```dotenv
# .env
DATABASE_URL=var/proof.db
```

> **Note (Symfony):** put a **plain filename** in `DATABASE_URL` — not a `sqlite://` DSN.
> The recipe does not translate schemes for custom `driver_class`; the driver
> handles the filename itself. Since 1.3.1 the driver also falls back to `dbname`
> automatically when dbal/Symfony inject the recipe-default `url: :memory:`, so
> persistence through the recipe works out of the box.

Migrations work through doctrine/migrations like any other DBAL connection.
A full proof harness — fresh Symfony 7.4 app, real migration, ORM round trip,
transaction rollback/commit — lives in [`docker/symfony-proof/`](docker/symfony-proof/).
It reports a single machine-parseable line:

```
PROOF-SYMFONY: {"ok":10,"fail":0,"all_ok":true}
```

## What's verified

Everything on this page that is claimed as *verified* is reproduced by CI or a
proof harness in this repository — nothing here is aspirational.

- **CI matrix** ([GitHub Actions](https://github.com/tursodatabase/turso-doctrine-dbal/actions)):
  PHP 8.2 / 8.3 / 8.4 / 8.5 × doctrine/dbal **4.0.2**, **^4.5** and **5.0.x-dev**
  — 10 legs, sqlite seeded on 8081/8082 for embedded-replica tests
- **Test suite**: [`./vendor/bin/phpunit`](composer.json) — feature tests for
  DDL/DML, streaming fetch, multi-row iteration, transactions, transactional
  reads, `lastInsertId()`, `getColumnName()`; unit tests for connection-mode
  detection and quoting
- **Symfony proof**: [`docker/symfony-proof/proof.sh`](docker/symfony-proof/proof.sh)
  — fresh skeleton, real migration, 10 end-to-end assertions
- **Wire protocol**: libSQL extension 1.7.0 source-of-truth (see
  [turso-client-php](https://github.com/tursodatabase/turso-client-php))

## Example

See [`example/index.php`](example/index.php) for a runnable smoke application
(create table → insert → select → print).

## Limitations

Explicitly not hidden (all tracked as issues):

- **Binary/BLOB parameters bind as NULL** — binding raw binary strings
  (`ParameterType::BINARY`) stores NULL instead of the bytes (extension
  limitation). Workaround: base64-encode payloads or use `x'<hex>'` literals.
  See [issue #3](https://github.com/tursodatabase/turso-doctrine-dbal/issues/3).
- **No nested transactions or savepoints** — the extension does not expose
  savepoints; nested `beginTransaction()` calls are not supported
- **No offline-write mode** — `sqld_offline_write` / `offline_writes`
  (libSQL's offline-write beta) is not wired up in this driver; see the
  Laravel driver's proof for the equivalent scope
- **`getColumnName()`** on `Doctrine\DBAL\Result` requires dbal ≥ 4.1.0
- **`:memory:` persistence trap on Symfony** — prior to 1.3.1, Symfony
  injected `url: :memory:` and `driverClass` lost the DSN and opened
  in-memory databases per process; fixed in 1.3.1 via `dbname` fallback

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## Testing

```bash
composer install
./vendor/bin/phpunit
```

Remote/replica tests are skipped by default. To run them:

```bash
TURSO_SQLD_AVAILABLE=true ./vendor/bin/phpunit
```

(requires a seeded sqld on `127.0.0.1:8081` / `8082` — see CI for the seed
script and `docker/proof` in the Laravel driver for a self-contained harness).

## Security

If you find a security vulnerability, please do **not** open a public issue.
Report it privately via a GitHub security advisory (or to the maintainer via
the email in `composer.json`), and we will coordinate a fix.

## Contributing

See [open issues](https://github.com/tursodatabase/turso-doctrine-dbal/issues).
Changes should pass CI and the proof harness, with tests for any behavior
change. The house style is: prove it first, then document it.

## License

MIT — see [LICENSE](LICENSE).
