<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL;

use Doctrine\DBAL\Driver\Result as ResultInterface;
use LibSQL;
use LibSQLResult;

final class Result implements ResultInterface
{
    /** @var list<list<mixed>>|null */
    private ?array $numericRows = null;

    /** @var list<array<string, mixed>>|null */
    private ?array $associativeRows = null;

    private function __construct(
        private readonly ?LibSQLResult $result,
        private readonly int|string $rowCount,
    ) {
    }

    public static function forRead(LibSQLResult $result): self
    {
        return new self($result, 0);
    }

    public static function forWrite(int|string $rowCount): self
    {
        return new self(null, $rowCount);
    }

    public function fetchNumeric(): array|false
    {
        $this->numericRows ??= $this->readNumericRows();

        return array_shift($this->numericRows) ?: false;
    }

    public function fetchAssociative(): array|false
    {
        $this->associativeRows ??= $this->readAssociativeRows();

        return array_shift($this->associativeRows) ?: false;
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
        $rows = $this->numericRows ??= $this->readNumericRows();
        $this->numericRows = [];

        return $rows;
    }

    public function fetchAllAssociative(): array
    {
        $rows = $this->associativeRows ??= $this->readAssociativeRows();
        $this->associativeRows = [];

        return $rows;
    }

    public function fetchFirstColumn(): array
    {
        return array_column($this->fetchAllNumeric(), 0);
    }

    public function rowCount(): int|string
    {
        if ($this->result === null) {
            return $this->rowCount;
        }

        return count($this->numericRows ??= $this->readNumericRows());
    }

    public function columnCount(): int
    {
        return $this->result?->numColumns() ?? 0;
    }

    public function free(): void
    {
        if ($this->result !== null) {
            if (method_exists($this->result, 'finalize')) {
                $this->result->finalize();
            } elseif (method_exists($this->result, 'reset')) {
                $this->result->reset();
            }
        }

        $this->numericRows = [];
        $this->associativeRows = [];
    }

    public function getColumnName(int $index): string
    {
        if ($this->result === null) {
            throw new \LogicException('Column names are only available for read results.');
        }

        return $this->result->columnName($index);
    }

    /** @return list<list<mixed>> */
    private function readNumericRows(): array
    {
        if ($this->result === null) {
            return [];
        }

        $rows = $this->result->fetchArray(LibSQL::LIBSQL_NUM);

        return is_array($rows) ? array_values($rows) : [];
    }

    /** @return list<array<string, mixed>> */
    private function readAssociativeRows(): array
    {
        if ($this->result === null) {
            return [];
        }

        $rows = $this->result->fetchArray(LibSQL::LIBSQL_ASSOC);

        return is_array($rows) ? array_values($rows) : [];
    }
}
