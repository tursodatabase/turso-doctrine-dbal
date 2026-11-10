<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL;

use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Doctrine\DBAL\ParameterType;

final class Statement implements StatementInterface
{
    /** @var array<int|string, mixed> */
    private array $parameters = [];

    public function __construct(
        private readonly Connection $connection,
        private readonly string $sql,
    ) {
    }

    public function bindValue(int|string $param, mixed $value, ParameterType $type): void
    {
        if (is_int($param)) {
            $this->parameters[$param - 1] = $value;

            return;
        }

        $this->parameters[$param] = $value;
    }

    public function execute(): Result
    {
        ksort($this->parameters);
        $result = $this->connection->executeStatement($this->sql, $this->normalizeParameters());
        $this->parameters = [];

        return $result;
    }

    /** @return array<int|string, mixed> */
    private function normalizeParameters(): array
    {
        foreach (array_keys($this->parameters) as $key) {
            if (is_string($key)) {
                return $this->parameters;
            }
        }

        return array_values($this->parameters);
    }
}
