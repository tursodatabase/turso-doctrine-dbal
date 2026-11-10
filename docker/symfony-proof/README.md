# Symfony Proof of Usage — Turso Doctrine DBAL

The fastest honest way to verify that `tursodatabase/turso-doctrine-dbal`
works inside a real Symfony application, before installing it in yours.

This harness builds a **fresh Symfony 7.4 application** inside Docker — the
driver is installed exactly the way you would install it (composer from the
package tree or from Packagist), the libSQL PHP extension is installed from the
official release tarball, `doctrine/migrations` runs a real migration, and a
console command (`proof:run`) asserts the integration end to end. Nothing is
installed on your host beyond Docker itself.

## Quick start

```bash
git clone https://github.com/tursodatabase/turso-doctrine-dbal.git
cd turso-doctrine-dbal
docker/symfony-proof/proof.sh
```

Ending:

```
PROOF-SYMFONY: {"ok":10,"fail":0,"all_ok":true,...}
```

## What it proves

| # | Check | What it proves |
|---|-------|----------------|
| 1 | `dbal_select_1` | DBAL round-trip query through the driver |
| 2 | `orm_persist_flush_find` | Doctrine ORM `persist`/`flush`/`find` round trip |
| 3 | `orm_dql_count` | DQL query execution |
| 4 | `dbal_raw_insert` | migration built `books`, raw dbal insert works |
| 5 | `tx_rollback` | transaction rollback discards the write |
| 6 | `tx_commit` | transaction commit persists the write |
| 7 | `streaming_fetch_completes` | `fetchAssociative()` iterator advances correctly |
| 8 | `named_and_positional_params` | named (`:named`) and positional (`?`) bindings |
| 9 | `last_insert_id_numeric` | `lastInsertId()` returns a numeric string |
| 10 | `server_version` | the extension reports its version |

Also verified, not asserted:
- Migrations run through the driver on both a local file and remote mode are
  not exercised here (see repo CI for remote/replica tests).

## What it does NOT prove

- **Remote / embedded replica** modes — those are integration-tested in CI
  (`.github/workflows/tests.yml` against two seeded `libsql-server` instances)
  but not in this app. Extending the harness with a `--remote` flavor is a
  future task.
- **ORM relations / repositories with hydration** — the proof exercises the
  ORM through one entity (Book) and raw DQL; deeper ORM surface (relations,
  cascades) is not covered here.
- **Encryption at rest** — works through the extension but is not asserted.
- **Offline writes** — libSQL's offline-write mode is exercised elsewhere;
  see the Laravel driver's proof harness for the equivalent flow.

## Build args

| Arg | Default | Purpose |
|-----|---------|---------|
| `LIBSQL_VERSION` | `1.7.0` | libSQL extension version to install |
| `DRIVER_SOURCE` | `path` | `path` = install from this checkout; `packagist` = published package |
| `DRIVER_VERSION` | `*` | version constraint for the driver |

## Using locally without Docker

Requires PHP ≥ 8.2, Composer, and libsql_php ≥ 1.7.0 loaded (e.g. via
`PHP_INI_SCAN_DIR=...`). The app files in `app/` are the exact copies the
Docker build overlays; copy them into a fresh `symfony/skeleton:^7.4` project
plus `doctrine/doctrine-bundle doctrine/orm doctrine/migrations`, then:

```bash
rm -f var/proof.db
php bin/console doctrine:migrations:migrate --no-interaction
php bin/console proof:run
```
