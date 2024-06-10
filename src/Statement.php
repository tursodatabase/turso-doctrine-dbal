<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL;

use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Doctrine\DBAL\ParameterType;
use LibSQL;
use LibSQLStatement;

final class Statement implements StatementInterface
{
    protected array $parameters = [];

    public function __construct(
        private readonly LibSQL $connection,
        private readonly LibSQLStatement $statement,
        private readonly string $sql,
        private readonly bool $isStandAlone
    ) {
    }

    public function bindValue(int|string $param, mixed $value, ParameterType $type): void
    {
        if (!preg_match('/^[:@]/', (string) $param)) {
            $this->parameters[] = $value;
        } else {
            $this->parameters[$param] = $value;
        }
    }

    public function execute(): Result
    {

        $result = $this->connection->query($this->sql, $this->parameters);
        $this->reset();

        return new Result($result, $this->isStandAlone);
    }

    public function reset(): void
    {
        $this->parameters = [];
    }
}
