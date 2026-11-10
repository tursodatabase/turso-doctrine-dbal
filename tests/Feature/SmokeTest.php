<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL\Tests\Feature;

use Doctrine\DBAL\Connection;
use Composer\InstalledVersions;
use Doctrine\DBAL\DriverManager;
use PHPUnit\Framework\TestCase;
use Turso\Doctrine\DBAL\Driver;

final class SmokeTest extends TestCase
{
    private function conn(): Connection
    {
        return DriverManager::getConnection([
            'url' => ':memory:',
            'driverClass' => Driver::class,
        ]);
    }

    public function testDdlDmlAndFetching(): void
    {
        $conn = $this->conn();
        $conn->executeStatement('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');

        $n = $conn->executeStatement('INSERT INTO t (name, age) VALUES (?, ?)', ['positional', 1]);
        $this->assertSame(1, $n);

        $n = $conn->executeStatement('INSERT INTO t (name, age) VALUES (:name, :age)', ['name' => 'named', 'age' => 2]);
        $this->assertSame(1, $n);

        $rows = $conn->executeQuery('SELECT * FROM t ORDER BY id')->fetchAllAssociative();
        $this->assertCount(2, $rows);
        $this->assertSame('positional', $rows[0]['name']);
        $this->assertSame('named', $rows[1]['name']);

        $rows = $conn->executeQuery('SELECT * FROM t ORDER BY id')->fetchAllNumeric();
        $this->assertSame(1, $rows[0][0]);
        $this->assertSame('named', $rows[1][1]);

        $this->assertSame(2, $conn->executeQuery('SELECT count(*) FROM t')->fetchOne());
    }

    public function testStreamingFetchNeverReturnsSameRowTwice(): void
    {
        $conn = $this->conn();
        $conn->executeStatement('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        $conn->executeStatement("INSERT INTO t (name) VALUES ('a')");
        $conn->executeStatement("INSERT INTO t (name) VALUES ('b')");

        $stmt = $conn->executeQuery('SELECT * FROM t ORDER BY id');
        $seen = [];
        while ($row = $stmt->fetchAssociative()) {
            $seen[] = $row['name'];
            $this->assertLessThan(10, count($seen), 'fetchAssociative never advanced — infinite loop');
        }

        $this->assertSame(['a', 'b'], $seen);
        $this->assertFalse($stmt->fetchAssociative());
    }

    public function testFetchFirstColumn(): void
    {
        $conn = $this->conn();
        $conn->executeStatement('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        $conn->executeStatement("INSERT INTO t (name) VALUES ('x')");
        $conn->executeStatement("INSERT INTO t (name) VALUES ('y')");

        $this->assertSame(['x', 'y'], $conn->executeQuery('SELECT name FROM t ORDER BY id')->fetchFirstColumn());
    }

    public function testFreeInvalidatesResult(): void
    {
        $conn = $this->conn();
        $conn->executeStatement('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        $conn->executeStatement("INSERT INTO t (name) VALUES ('x')");

        $stmt = $conn->executeQuery('SELECT * FROM t');
        $stmt->free();

        $this->assertFalse($stmt->fetchAssociative());
    }

    public function testTransactions(): void
    {
        $conn = $this->conn();
        $conn->executeStatement('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');

        $conn->beginTransaction();
        $conn->executeStatement("INSERT INTO t (name) VALUES ('committed')");
        $conn->commit();
        $this->assertSame(1, $conn->executeQuery("SELECT count(*) FROM t WHERE name = 'committed'")->fetchOne());

        $conn->beginTransaction();
        $conn->executeStatement("INSERT INTO t (name) VALUES ('rolled-back')");
        $conn->rollBack();
        $this->assertSame(0, $conn->executeQuery("SELECT count(*) FROM t WHERE name = 'rolled-back'")->fetchOne());
    }

    public function testReadInsideTransaction(): void
    {
        $conn = $this->conn();
        $conn->executeStatement('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        $conn->executeStatement("INSERT INTO t (name) VALUES ('a')");

        $conn->beginTransaction();
        $count = $conn->executeQuery('SELECT count(*) FROM t')->fetchOne();
        $name = $conn->executeQuery('SELECT name FROM t')->fetchAssociative();
        $conn->commit();

        $this->assertSame(1, $count);
        $this->assertSame('a', $name['name']);
    }

    public function testLastInsertIdIsNumeric(): void
    {
        $conn = $this->conn();
        $conn->executeStatement('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        $conn->executeStatement("INSERT INTO t (name) VALUES ('x')");

        $id = (string) $conn->lastInsertId();
        $this->assertMatchesRegularExpression('/^\d+$/', $id);
    }

    public function testGetColumnName(): void
    {
        $version = InstalledVersions::getVersion('doctrine/dbal');
        if (version_compare((string) $version, '4.1.0', '<')) {
            $this->markTestSkipped('getColumnName() on Doctrine\DBAL\Result requires doctrine/dbal >= 4.1.0');
        }

        $conn = $this->conn();
        $conn->executeStatement('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        $conn->executeStatement("INSERT INTO t (name) VALUES ('x')");

        $stmt = $conn->executeQuery('SELECT id, name FROM t');
        $this->assertSame('id', $stmt->getColumnName(0));
        $this->assertSame('name', $stmt->getColumnName(1));
    }

    public function testRawBinaryBindingCurrentlyBindsAsNull(): void
    {
        $this->markTestSkipped('documents issue #3 — raw binary parameters bind as NULL in the extension; see https://github.com/tursodatabase/turso-doctrine-dbal/issues/3');
    }
}
