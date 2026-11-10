<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL\Tests\Feature;

use Doctrine\DBAL\DriverManager;
use PHPUnit\Framework\TestCase;
use Turso\Doctrine\DBAL\Driver;

final class RemoteReplicaTest extends TestCase
{
    private static function enabled(): bool
    {
        return filter_var(getenv('TURSO_SQLD_AVAILABLE'), FILTER_VALIDATE_BOOLEAN);
    }

    protected function setUp(): void
    {
        if (! self::enabled()) {
            $this->markTestSkipped('TURSO_SQLD_AVAILABLE not set — remote/replica tests skipped');
        }
    }

    public function testRemoteLineRoundTrip(): void
    {
        $conn = DriverManager::getConnection([
            'auth_token' => 'ci-dummy-token',
            'sync_url' => 'http://127.0.0.1:8081',
            'driverClass' => Driver::class,
        ]);

        $conn->executeStatement('CREATE TABLE IF NOT EXISTS doctrine_ci_remote_smoke (id INTEGER PRIMARY KEY, name TEXT)');
        $conn->executeStatement("INSERT INTO doctrine_ci_remote_smoke (name) VALUES ('ci-remote')");
        $rows = $conn->executeQuery('SELECT count(*) FROM doctrine_ci_remote_smoke')->fetchOne();
        $this->assertSame(1, $rows);
        $conn->executeStatement('DROP TABLE doctrine_ci_remote_smoke');
    }

    public function testRemoteReplicaRoundTrip(): void
    {
        $path = sys_get_temp_dir() . '/doctrine-replica-' . getmypid() . '.db';

        try {
            $conn = DriverManager::getConnection([
                'url' => $path,
                'auth_token' => 'ci-dummy-token',
                'sync_url' => 'http://127.0.0.1:8082',
                'driverClass' => Driver::class,
            ]);

            $conn->executeStatement('CREATE TABLE IF NOT EXISTS doctrine_ci_replica_smoke (id INTEGER PRIMARY KEY, name TEXT)');
            $conn->executeStatement("INSERT INTO doctrine_ci_replica_smoke (name) VALUES ('ci-replica')");
            $rows = $conn->executeQuery('SELECT count(*) FROM doctrine_ci_replica_smoke')->fetchOne();
            $this->assertSame(1, $rows);
            $conn->executeStatement('DROP TABLE doctrine_ci_replica_smoke');
        } finally {
            foreach (glob($path . '*') ?: [] as $file) {
                @unlink($file);
            }
        }
    }
}
