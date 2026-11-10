<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL\Tests\Unit;

use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Turso\Doctrine\DBAL\Driver;

final class ConnectionModeDetectionTest extends TestCase
{
    private function mode(array $params): string
    {
        $driver = new Driver();
        $method = new ReflectionMethod($driver, 'getConnectionMode');
        $method->setAccessible(true);

        return $method->invoke($driver, $params);
    }

    public function testReplicaWhenLocalFileUrlHasSyncUrl(): void
    {
        $this->assertSame('remote_replica', $this->mode(['url' => 'database.db', 'sync_url' => 'http://127.0.0.1:8082', 'auth_token' => 'x']));
    }

    public function testRemoteWhenSyncUrlAndNoLocalFile(): void
    {
        $this->assertSame('remote', $this->mode(['url' => ':memory:', 'sync_url' => 'http://127.0.0.1:8081', 'auth_token' => 'x']));
    }

    public function testLocalWhenFileUrlWithoutSyncUrl(): void
    {
        $this->assertSame('local', $this->mode(['url' => 'database.db']));
        $this->assertSame('local', $this->mode(['url' => 'database.sqlite']));
    }

    public function testLocalFromDbnameWhenUrlConsumedByDsnParser(): void
    {
        // dbal's DriverManager consumes `url` at connection-build time and
        // forwards dbname instead — see src/Driver.php::resolveDatabaseUrl().
        $this->assertSame('local', $this->mode(['dbname' => 'var/proof.db']));
        $this->assertSame('remote_replica', $this->mode(['dbname' => 'var/replica.db', 'sync_url' => 'http://127.0.0.1:8082']));
        $this->assertSame('memory', $this->mode(['dbname' => '']));
    }

    public function testMemoryWhenNothingMatches(): void
    {
        $this->assertSame('memory', $this->mode(['url' => ':memory:']));
        $this->assertSame('memory', $this->mode([]));
    }

    public function testReplicaWithoutAuthTokenStillClassifiesAsReplica(): void
    {
        // Documented behavior of getConnectionMode(): classification keys off
        // url + sync_url only. Connection with a bad/empty token will fail at
        // connect(), loudly.
        $this->assertSame('remote_replica', $this->mode(['url' => 'database.db', 'sync_url' => 'http://127.0.0.1:8082']));
    }
}
