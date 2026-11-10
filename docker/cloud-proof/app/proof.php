<?php

declare(strict_types=1);

/*
 * Turso Cloud offline-writes proof.
 *
 * Flow:
 *   1. open offline-writes embedded replica against Turso Cloud
 *   2. bootstrap sync() — pulls remote state BEFORE any write
 *   3. DDL+INSERT journal locally (replica file, never written to cloud)
 *   4. local count == 2 (reads own journal)
 *   5. sync() pushes journal to cloud
 *   6. plain remote read-back (independent connection) == 2
 *   7. DROP TABLE through replica, sync() push
 *   8. plain remote confirms table gone
 *   9. cloud database left clean (zero app tables)
 *
 * Exits 0 on all_ok, 1 otherwise. Prints PROOF-CLOUD: {json}.
 */

require '/app/vendor/autoload.php';

use Doctrine\DBAL\DriverManager;
use Turso\Doctrine\DBAL\Driver;

$url = getenv('TURSO_CLOUD_DB_URL');
$token = getenv('TURSO_CLOUD_DB_TOKEN');
$replica = '/tmp/cloud-proof-' . bin2hex(random_bytes(4)) . '.db';
$table = 'dbal_cloud_proof_' . bin2hex(random_bytes(6));

if ($url === false || $url === '' || $token === false || $token === '') {
    fwrite(STDERR, "TURSO_CLOUD_DB_URL / TURSO_CLOUD_DB_TOKEN not set\n");
    exit(2);
}

$checks = [];
$check = static function (string $name, callable $fn) use (&$checks): bool {
    try {
        $checks[$name] = (bool) $fn();
    } catch (Throwable $e) {
        file_put_contents('php://stderr', "check {$name} threw: " . $e->getMessage() . "\n");
        $checks[$name] = false;
    }

    return $checks[$name];
};

$offlineReplica = fn () => DriverManager::getConnection([
    'url' => $replica,
    'auth_token' => $token,
    'sync_url' => $url,
    'sync_interval' => 0,
    'read_your_writes' => true,
    'driverClass' => Driver::class,
    'sqld_offline_mode' => false,
    'offline_writes' => true,
]);

$remote = fn () => DriverManager::getConnection([
    'url' => ':memory:',
    'auth_token' => $token,
    'sync_url' => $url,
    'driverClass' => Driver::class,
]);

try {
    $conn = $offlineReplica();

    // 1) bootstrap pull — BEFORE any write (frames must have a remote base)
    $conn->getNativeConnection()->sync();

    // 2) journaled DDL+INSERT on the local replica
    $conn->executeStatement("CREATE TABLE {$table} (id INTEGER PRIMARY KEY, name TEXT)");
    $conn->executeStatement("INSERT INTO {$table} (name) VALUES ('proof-a')");
    $conn->executeStatement("INSERT INTO {$table} (name) VALUES ('proof-b')");

    $check('journal_writes_local', fn () => $conn->executeQuery("SELECT count(*) FROM {$table}")->fetchOne() === 2);

    // 3) push — journaled frames upload to the cloud
    $conn->getNativeConnection()->sync();
    $conn->close();

    // 4) plain remote read-back — independent connection, not the replica
    $remoteConn = $remote();
    $check('remote_read_back', fn () => $remoteConn->executeQuery("SELECT count(*) FROM {$table} WHERE name LIKE 'proof-%'")->fetchOne() === 2);

    // 5) DROP through the offline replica, push
    $conn = $offlineReplica();
    $conn->executeStatement("DROP TABLE IF EXISTS {$table}");
    $conn->getNativeConnection()->sync();
    $conn->close();

    // 6) remote confirms gone
    $check('remote_table_gone', fn () => $remoteConn->executeQuery(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = '{$table}'"
    )->fetchOne() === 0);

    $remoteConn->close();
} catch (Throwable $e) {
    $checks['exception'] = false;
    file_put_contents('php://stderr', "exception: " . $e->getMessage() . "\n");
} finally {
    // belt-and-braces: remote DROP regardless of what succeeded
    try {
        $remoteConn = $remote();
        $remoteConn->executeStatement("DROP TABLE IF EXISTS {$table}");
        $remoteConn->close();
    } catch (Throwable) {
        // best effort
    }

    foreach (glob($replica . '*') ?: [] as $file) {
        @unlink($file);
    }
}

$ok = count(array_filter($checks));
$fail = count($checks) - $ok;

echo 'PROOF-CLOUD: ' . json_encode(['ok' => $ok, 'fail' => $fail, 'all_ok' => $fail === 0, 'checks' => $checks]) . "\n";

exit($fail === 0 ? 0 : 1);
