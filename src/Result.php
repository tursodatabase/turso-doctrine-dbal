<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL;

use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Exception\NoKeyValue;
use LibSQL;
use LibSQLResult;

final class Result implements ResultInterface
{
    private LibSQLResult $result;

    public function __construct(
        LibSQLResult $result,
        private readonly bool $isStandAlone
    ) {
        $this->result = $result;
    }

    public function fetchNumeric(): array|false
    {
        return current($this->result->fetchArray(LibSQL::LIBSQL_NUM));
    }

    public function fetchAssociative(): array|false
    {
        return current($this->result->fetchArray(LibSQL::LIBSQL_ASSOC));
    }

    public function fetchOne(): mixed
    {
        $row = $this->fetchNumeric();

        if ($row === false) {
            return false;
        }

        return $row[0];
    }

    public function fetchAllNumeric(): array
    {
        if ($this->fetchNumeric() === false) {
            return [];
        }

        return array_map(function ($row) {
            return $row;
        }, $this->fetchNumeric());
    }

    public function fetchAllAssociative(): array
    {
        if ($this->result->fetchArray(LibSQL::LIBSQL_ASSOC) === false) {
            return [];
        }

        return array_map(function ($row) {
            return $row;
        }, $this->result->fetchArray(LibSQL::LIBSQL_ASSOC));
    }

    public function fetchAllKeyValue(): array
    {
        $this->ensureHasKeyValue();

        $data = [];

        foreach ($this->fetchAllNumeric() as $row) {
            assert(count($row) >= 2);
            [$key, $value] = $row;
            $data[$key]    = $value;
        }

        return $data;
    }

    public function fetchAllAssociativeIndexed(): array
    {
        $data = [];

        foreach ($this->fetchAllAssociative() as $row) {
            $data[array_shift($row)] = $row;
        }

        return $data;
    }

    public function fetchFirstColumn(): array
    {
        return array_map(function ($row) {
            return $row;
        }, $this->fetchOne());
    }

    public function rowCount(): int
    {
        return count($this->result->fetchArray(LibSQL::LIBSQL_NUM));
    }

    public function columnCount(): int
    {
        return $this->result->numColumns();
    }

    public function free(): void
    {
        $this->result = $this->result;
    }

    private function ensureHasKeyValue(): void
    {
        $columnCount = $this->columnCount();

        if ($columnCount < 2) {
            throw NoKeyValue::fromColumnCount($columnCount);
        }
    }
}
