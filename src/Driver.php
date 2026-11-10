<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL;

use Doctrine\DBAL\Driver as DbalDriverInterface;
use Doctrine\DBAL\Driver\API\SQLite\ExceptionConverter;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Doctrine\DBAL\ServerVersionProvider;
use LibSQL;
use SensitiveParameter;

final class Driver implements DbalDriverInterface
{

    public function connect(
        #[SensitiveParameter]
        array $params,
    ): Connection {
        $isStandAlone = true;

        $driverOptions = $params['driverOptions'] ?? $params['options'] ?? [];

        if (
            ($driverOptions['use_framework'] ?? false) === true &&
            array_key_exists('url', $driverOptions) &&
            array_key_exists('auth_token', $driverOptions) &&
            array_key_exists('sync_url', $driverOptions)
        ) {
            $params['url'] = str_replace('sqlite:///', '', (string) $driverOptions['url']);
            $params['auth_token'] = (string) $driverOptions['auth_token'];
            $params['sync_url'] = (string) $driverOptions['sync_url'];
            $isStandAlone = false;
        } elseif (
            ($driverOptions['use_framework'] ?? false) === true &&
            array_key_exists('auth_token', $driverOptions) &&
            array_key_exists('sync_url', $driverOptions)
        ) {
            $params['url'] = $params['url'] ?? ':memory:';
            $params['auth_token'] = (string) $driverOptions['auth_token'];
            $params['sync_url'] = (string) $driverOptions['sync_url'];
            $isStandAlone = false;
        } elseif (($driverOptions['use_framework'] ?? false) === true && isset($params['path'])) {
            $params['url'] = str_replace('sqlite:///', '', (string) $params['path']);
            $isStandAlone = false;
        } elseif (($driverOptions['use_framework'] ?? false) === true && isset($params['memory'])) {
            $params['url'] = ':memory:';
            $isStandAlone = false;
        }

        $params['url'] ??= ':memory:';

        // Offline-write mode flags (see docs/offline-writes). Both must be
        // explicitly enabled; the sqld flavor additionally requires the
        // connection to be opened with sqld_offline_mode.
        $sqldOfflineMode = filter_var($params['sqld_offline_mode'] ?? false, FILTER_VALIDATE_BOOLEAN);
        $offlineWrites = filter_var($params['offline_writes'] ?? false, FILTER_VALIDATE_BOOLEAN);

        $mode = $this->getConnectionMode($params);

        try {
            switch ($mode) {
                case 'remote_replica':
                    $defaultParams = [
                        'sync_interval' => 5,
                        'read_your_writes' => true,
                        'encryption_key' => '',
                    ];

                    $params['url'] = 'file:' . $this->resolveDatabaseUrl($params);
                    $config = array_merge($params, $defaultParams);

                    $databaseConfig = [
                        'url' => $config['url'],
                        'authToken' => $config['auth_token'],
                        'syncUrl' => $config['sync_url'],
                        'syncInterval' => $config['sync_interval'],
                        'read_your_writes' => $config['read_your_writes'],
                        'encryptionKey' => $config['encryption_key'],
                    ];

                    $connection = $this->connectLibSQL($databaseConfig, $sqldOfflineMode, $offlineWrites, (string) $config['encryption_key']);
                    break;

                case 'remote':
                    $connection = $this->connectLibSQL(sprintf(
                        'libsql:dbname=%s;authToken=%s',
                        $params['sync_url'],
                        $params['auth_token'],
                    ), $sqldOfflineMode, $offlineWrites, '');
                    break;

                case 'local':
                    $encryptionKey = !empty($params['encryption_key']) ? (string) $params['encryption_key'] : '';
                    $database = 'file:' . $this->resolveDatabaseUrl($params);
                    $connection = $this->connectLibSQL(
                        "libsql:dbname=$database",
                        $sqldOfflineMode,
                        $offlineWrites,
                        $encryptionKey,
                    );
                    break;

                case 'memory':
                    $connection = $this->connectLibSQL(':memory:', $sqldOfflineMode, $offlineWrites, '');
                    break;

                default:
                    throw new \RuntimeException('Connection mode is not found.');
            }
        } catch (\Exception $e) {
            throw Exception::new($e);
        }

        return new Connection($connection, $isStandAlone);
    }

    public function getDatabasePlatform(ServerVersionProvider $versionProvider): SQLitePlatform
    {
        // No readonly mismatch to inherit: this package implements the plain
        // Doctrine\DBAL\Driver contract, identical across dbal 4.x and 5.0.
        return new SQLitePlatform();
    }

    public function getExceptionConverter(): ExceptionConverter
    {
        return new ExceptionConverter();
    }
    /**
     * dbal's DriverManager consumes `url` at connection-build time (DSN parse)
     * and forwards the remaining params with `dbname` set instead. Fall back to
     * `dbname` so embedded replicas / local files still resolve correctly —
     * without this, every dbal user that configured `url` got a silent
     * `:memory:` connection instead of their file.
     */
    /**
     * Feature-detected LibSQL constructor wiring — mirrors what the Laravel
     * adapter does for the same extension API. The extension gained the
     * offline flags in 1.7.0; older payloads keep working with 1-arg/4-arg
     * fallbacks.
     */
    private function connectLibSQL(string|array $config, bool $sqldOfflineMode, bool $offlineWrites, string $encryptionKey): LibSQL
    {
        static $paramCount = null;

        if ($paramCount === null) {
            $paramCount = (new \ReflectionClass(LibSQL::class))
                ->getMethod('__construct')
                ->getNumberOfParameters();
        }

        return match (true) {
            $paramCount >= 5 => new LibSQL($config, $sqldOfflineMode, LibSQL::OPEN_READWRITE | LibSQL::OPEN_CREATE, $encryptionKey, $offlineWrites),
            $paramCount === 4 => new LibSQL($config, LibSQL::OPEN_READWRITE | LibSQL::OPEN_CREATE, $encryptionKey, $offlineWrites),
            default => new LibSQL($config),
        };
    }

    private function resolveDatabaseUrl(array $params): string
    {
        $url = (string) ($params['url'] ?? '');

        if ($url !== '' && $url !== ':memory:') {
            return $this->normalizeFileUrl($url);
        }

        // Symfony/dbal feeds the file as `dbname` and injects a default
        // `url: ':memory:'` when the user supplies no scheme — prefer the
        // real filename over the synthesized default, otherwise every
        // Symfony user silently loses persistence.
        $dbname = (string) ($params['dbname'] ?? '');

        if ($dbname !== '') {
            return $this->normalizeFileUrl($dbname);
        }

        return ':memory:';
    }

    private function normalizeFileUrl(string $url): string
    {
        if (str_starts_with($url, 'file://')) {
            return substr($url, 7);
        }

        if (str_starts_with($url, 'file:')) {
            return substr($url, 5);
        }

        return $url;
    }

    private function getConnectionMode(array $params): string
    {
        $url = $this->resolveDatabaseUrl($params);
        $syncUrl = (string) ($params['sync_url'] ?? '');

        if (
            $url !== '' &&
            $this->containsAny($url, ['.db', '.sqlite']) &&
            $syncUrl !== ''
        ) {
            return 'remote_replica';
        }

        if ($syncUrl !== '') {
            return 'remote';
        }

        if ($url !== '' && $this->containsAny($url, ['.db', '.sqlite'])) {
            return 'local';
        }

        return 'memory';
    }

    private function containsAny(string $haystack, array $needles): bool
    {
        foreach ($needles as $needle) {
            if (str_contains($haystack, $needle)) {
                return true;
            }
        }

        return false;
    }
}
