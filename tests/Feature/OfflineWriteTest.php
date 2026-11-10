<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL\Tests\Feature;

use Doctrine\DBAL\DriverManager;
use PHPUnit\Framework\TestCase;
use Turso\Doctrine\DBAL\Driver;

/**
 * Offline-write integration. Set TURSO_SQLD_AVAILABLE=true to enable.
 *
 * Two flavors are exercised:
 *  - online sqld with the offline flags accepted (writes stream through);
 *  - offline mode against an unreachable endpoint (writes journal locally);
 *    the same replica file is then reopened against the real server and
 *    sync() drains the journal.
 */
final class OfflineWriteTest extends TestCase
{
    private const REMOTE_URL = 'http://127.0.0.1:8082';
    private const UNUSED_PORT = 'http://127.0.0.1:8099';

    private static function enabled(): bool
    {
        return filter_var(getenv('TURSO_SQLD_AVAILABLE'), FILTER_VALIDATE_BOOLEAN);
    }

    protected function setUp(): void
    {
        if (! self::enabled()) {
            $this->markTestSkipped('TURSO_SQLD_AVAILABLE not set — offline-write tests skipped');
        }
    }

    private static function table(): string
    {
        return 'doctrine_offline_' . getmypid() . '_' . random_int(1000, 9999);
    }

    private static function offlineReplica(string $path, string $syncUrl): \Doctrine\DBAL\Connection
    {
        return DriverManager::getConnection([
            'url' => $path,
            'auth_token' => 'ci-dummy-token',
            'sync_url' => $syncUrl,
            'sync_interval' => 0,
            'read_your_writes' => true,
            'driverClass' => Driver::class,
            'sqld_offline_mode' => true,
            'offline_writes' => true,
        ]);
    }

    private static function remote(): \Doctrine\DBAL\Connection
    {
        return DriverManager::getConnection([
            'url' => ':memory:',
            'auth_token' => 'ci-dummy-token',
            'sync_url' => self::REMOTE_URL,
            'driverClass' => Driver::class,
        ]);
    }

    public function testOnlineReplicaAcceptsOfflineFlags(): void
    {
        $path = sys_get_temp_dir() . '/doctrine-offline-online-' . getmypid() . '.db';
        $table = self::table();

        try {
            $conn = self::offlineReplica($path, self::REMOTE_URL);

            $conn->executeStatement("CREATE TABLE {$table} (id INTEGER PRIMARY KEY, name TEXT)");
            $conn->executeStatement("INSERT INTO {$table} (name) VALUES ('online')");

            $this->assertTrue($conn->getNativeConnection()->isOnline());
            $this->assertSame(0, $conn->getNativeConnection()->getPendingOperationsCount(), 'online sqld applies immediately');

            // sync() is a no-op on online replicas but must not error
            $conn->getNativeConnection()->sync();

            $this->assertSame(1, $conn->executeQuery("SELECT count(*) FROM {$table}")->fetchOne());
            $conn->close();

            $remote = self::remote();
            $this->assertSame(1, $remote->executeQuery("SELECT count(*) FROM {$table} WHERE name = 'online'")->fetchOne());
            $remote->close();
        } finally {
            try {
                $remote = self::remote();
                $remote->executeStatement("DROP TABLE IF EXISTS {$table}");
                $remote->close();
            } catch (\Throwable) {
            }

            foreach (glob($path . '*') ?: [] as $file) {
                @unlink($file);
            }
        }
    }

    public function testOfflineJournalDrainsAfterOutage(): void
    {
        $path = sys_get_temp_dir() . '/doctrine-offline-drain-' . getmypid() . '.db';
        $table = self::table();

        try {
            // 1) Open against an unreachable endpoint — the server is down
            $conn = self::offlineReplica($path, self::UNUSED_PORT);

            $this->assertFalse($conn->getNativeConnection()->isOnline(), 'server unreachable — extension reports offline');
            $this->assertSame(0, $conn->getNativeConnection()->getPendingOperationsCount());

            $conn->executeStatement("CREATE TABLE {$table} (id INTEGER PRIMARY KEY, name TEXT)");
            $this->assertSame(1, $conn->getNativeConnection()->getPendingOperationsCount(), 'offline DDL journals locally');

            $conn->executeStatement("INSERT INTO {$table} (name) VALUES ('journaled')");
            $this->assertSame(2, $conn->getNativeConnection()->getPendingOperationsCount(), 'offline DML journals locally');

            // Local replica reads its own journaled rows immediately
            $this->assertSame(1, $conn->executeQuery("SELECT count(*) FROM {$table}")->fetchOne());
            $conn->close();

            // 2) Reopen against the real server and drain the journal
            $conn = self::offlineReplica($path, self::REMOTE_URL);
            $conn->getNativeConnection()->sync();
            $this->assertSame(0, $conn->getNativeConnection()->getPendingOperationsCount(), 'sync() drains the journal to the server');
            $conn->close();

            // 3) Verify the server now has the data
            $remote = self::remote();
            $this->assertSame(1, $remote->executeQuery("SELECT count(*) FROM {$table} WHERE name = 'journaled'")->fetchOne());
            $remote->close();
        } finally {
            try {
                $remote = self::remote();
                $remote->executeStatement("DROP TABLE IF EXISTS {$table}");
                $remote->close();
            } catch (\Throwable) {
            }

            foreach (glob($path . '*') ?: [] as $file) {
                @unlink($file);
            }
        }
    }
}
