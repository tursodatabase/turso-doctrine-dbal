<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL\Tests\Feature;

use Doctrine\DBAL\DriverManager;
use PHPUnit\Framework\TestCase;
use Turso\Doctrine\DBAL\Driver;

/**
 * Turso Cloud offline-write flavor proof — opt-in only.
 *
 * This exercises the Turso Cloud-only journaled-writes flavor:
 *   sqld_offline_mode=false, offline_writes=true, embedded replica with syncInterval=0.
 *
 * Unlike the sqld flavor, it needs a real Turso Cloud database; there is no
 * way to emulate it locally. Skipped unless both env vars are set:
 *
 *   TURSO_CLOUD_DB_URL=libsql://<db>-<org>.turso.io
 *   TURSO_CLOUD_DB_TOKEN=<token>
 *
 * The test creates and drops its own table (dbal_proof_cloud_ow_*) and always
 * attempts cleanup, including a belt-and-braces plain-remote DROP.
 */
final class TursoCloudOfflineWriteTest extends TestCase
{
    private const ENV_URL = 'TURSO_CLOUD_DB_URL';
    private const ENV_TOKEN = 'TURSO_CLOUD_DB_TOKEN';

    private static function credentials(): ?array
    {
        $url = getenv(self::ENV_URL);
        $token = getenv(self::ENV_TOKEN);

        return ($url !== false && $url !== '' && $token !== false && $token !== '')
            ? ['url' => $url, 'token' => $token]
            : null;
    }

    protected function setUp(): void
    {
        if (self::credentials() === null) {
            $this->markTestSkipped('TURSO_CLOUD_DB_URL / TURSO_CLOUD_DB_TOKEN not set — needs a real Turso Cloud database');
        }
    }

    public function testCloudOfflineWriteJournalPushesToRemote(): void
    {
        $creds = self::credentials();
        $table = 'dbal_proof_cloud_ow_' . bin2hex(random_bytes(6));
        $replicaFile = sys_get_temp_dir() . '/doctrine-cloud-offline-' . getmypid() . '.db';

        $offlineReplica = fn () => DriverManager::getConnection([
            'url' => $replicaFile,
            'auth_token' => $creds['token'],
            'sync_url' => $creds['url'],
            'sync_interval' => 0,
            'read_your_writes' => true,
            'driverClass' => Driver::class,
            'sqld_offline_mode' => false,
            'offline_writes' => true,
        ]);

        $remote = fn () => DriverManager::getConnection([
            'url' => ':memory:',
            'auth_token' => $creds['token'],
            'sync_url' => $creds['url'],
            'driverClass' => Driver::class,
        ]);

        try {
            // 1) Open the offline-writes embedded replica against Turso Cloud
            $conn = $offlineReplica();

            // 2) Bootstrap: pull the remote state into the local replica BEFORE writing
            $conn->getNativeConnection()->sync();

            // 3) Journaled writes apply locally (never reach the cloud until sync())
            $conn->executeStatement("CREATE TABLE {$table} (id INTEGER PRIMARY KEY, name TEXT)");
            $conn->executeStatement("INSERT INTO {$table} (name) VALUES (?)", ['cloud-offline-a']);
            $conn->executeStatement("INSERT INTO {$table} (name) VALUES (?)", ['cloud-offline-b']);

            $this->assertSame(2, $conn->executeQuery("SELECT count(*) FROM {$table}")->fetchOne());

            // 4) sync() pushes the journaled frames to Turso Cloud
            $conn->getNativeConnection()->sync();
            $conn->close();

            // 5) Verify from a separate plain remote connection — independent of the replica
            $remoteConn = $remote();
            $this->assertSame(2, $remoteConn->executeQuery("SELECT count(*) FROM {$table} WHERE name LIKE 'cloud-offline-%'")->fetchOne());
            $remoteConn->close();
        } finally {
            // 6) Cleanup through the offline-writes path, push, then belt-and-braces plain remote DROP
            try {
                $conn = $offlineReplica();
                $conn->executeStatement("DROP TABLE IF EXISTS {$table}");
                $conn->getNativeConnection()->sync();
                $conn->close();
            } catch (\Throwable $e) {
                error_log('cloud offline-write cleanup (offline path) failed: ' . $e->getMessage());
            }

            try {
                $remoteConn = $remote();
                $remoteConn->executeStatement("DROP TABLE IF EXISTS {$table}");
                $remoteConn->close();
            } catch (\Throwable $e) {
                error_log('cloud offline-write cleanup (remote) failed: ' . $e->getMessage());
            }

            foreach (glob($replicaFile . '*') ?: [] as $file) {
                @unlink($file);
            }
        }
    }
}
