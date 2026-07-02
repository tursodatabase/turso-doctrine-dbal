<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL;

use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use LibSQL;
use LibSQLTransaction;

final class Connection implements ConnectionInterface
{
    private bool $inTransaction = false;
    private ?LibSQLTransaction $transaction = null;

    public function __construct(
        private readonly LibSQL $connection,
        private readonly bool $isStandAlone,
    ) {
    }

    public function prepare(string $sql): Statement
    {
        return new Statement($this, $sql);
    }

    public static function escapeString($value)
    {
        // DISCUSSION: Open PR if you have best approach
        $escaped_value = str_replace(
            ["\\", "\x00", "\n", "\r", "\x1a", "'", '"'],
            ["\\\\", "\\0", "\\n", "\\r", "\\Z", "\\'", '\\"'],
            $value
        );

        return $escaped_value;
    }

    public function quote(string $value): string
    {
        return self::escapeString($value);
    }

    public function query(string $sql): Result
    {
        try {
            if ($this->isReadQuery($sql)) {
                return Result::forRead($this->executeReadQuery($sql, []));
            }

            $affectedRows = $this->currentExecutor()->execute($sql);

            return Result::forWrite($affectedRows);
        } catch (\Exception $e) {
            throw Exception::new($e);
        }
    }

    public function exec(string $sql): int|string
    {
        try {
            return $this->currentExecutor()->execute($sql);
        } catch (\Exception $e) {
            throw Exception::new($e);
        }
    }

    public function lastInsertId(): int|string
    {
        try {
            if ($this->inTransaction) {
                $result = $this->transaction?->query('SELECT last_insert_rowid()', []);

                if (is_array($result)) {
                    return (int) ($result['last_insert_rowid'] ?? 0);
                }

                $row = $result?->fetchSingle(LibSQL::LIBSQL_NUM);

                return is_array($row) ? (int) $row[0] : 0;
            }

            return $this->connection->lastInsertedId();
        } catch (\Exception $e) {
            throw Exception::new($e);
        }
    }

    public function beginTransaction(): void
    {
        try {
            $this->transaction = $this->connection->transaction();
            $this->inTransaction = true;
        } catch (\Exception $e) {
            throw Exception::new($e);
        }
    }

    public function commit(): void
    {
        try {
            $this->transaction?->commit();
            $this->transaction = null;
            $this->inTransaction = false;
        } catch (\Exception $e) {
            throw Exception::new($e);
        }
    }

    public function rollBack(): void
    {
        try {
            $this->transaction?->rollback();
            $this->transaction = null;
            $this->inTransaction = false;
        } catch (\Exception $e) {
            throw Exception::new($e);
        }
    }

    public function getNativeConnection(): LibSQL
    {
        return $this->connection;
    }

    public function getServerVersion(): string
    {
        return LibSQL::version();
    }

    public function executeStatement(string $sql, array $parameters): Result
    {
        try {
            if ($this->isReadQuery($sql)) {
                return Result::forRead($this->executeReadQuery($sql, $parameters));
            }

            $affectedRows = $this->currentExecutor()->execute($sql, $parameters);

            return Result::forWrite($affectedRows);
        } catch (\Exception $e) {
            throw Exception::new($e);
        }
    }

    private function currentExecutor(): LibSQL|LibSQLTransaction
    {
        return $this->transaction ?? $this->connection;
    }

    private function executeReadQuery(string $sql, array $parameters): \LibSQLResult
    {
        if ($this->transaction !== null) {
            $result = $this->transaction->query($sql, $parameters);

            if (!$result instanceof \LibSQLResult) {
                throw new \RuntimeException('Expected LibSQLResult for transactional read query.');
            }

            return $result;
        }

        return $this->connection->query($sql, $parameters);
    }

    private function isReadQuery(string $sql): bool
    {
        return preg_match('/^(SELECT|PRAGMA|EXPLAIN|WITH)\b/i', ltrim($sql)) === 1;
    }
}
