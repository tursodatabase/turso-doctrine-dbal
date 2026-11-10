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

        try {
            switch ($this->getConnectionMode($params)) {
                case 'remote_replica':
                    $defaultParams = [
                        'sync_interval' => 5,
                        'read_your_writes' => true,
                        'encryption_key' => '',
                    ];

                    $params['url'] = 'file:'.$params['url'];
                    $config = array_merge($params, $defaultParams);

                    $databaseConfig = [
                        'url' => $config['url'],
                        'authToken' => $config['auth_token'],
                        'syncUrl' => $config['sync_url'],
                        'syncInterval' => $config['sync_interval'],
                        'read_your_writes' => $config['read_your_writes'],
                        'encryptionKey' => $config['encryption_key'],
                    ];

                    $connection = new LibSQL($databaseConfig);
                    break;

                case 'remote':
                    $connection = new LibSQL(sprintf(
                        'libsql:dbname=%s;authToken=%s',
                        $params['sync_url'],
                        $params['auth_token'],
                    ));
                    break;

                case 'local':
                    $encryptionKey = !empty($params['encryption_key']) ? (string) $params['encryption_key'] : '';
                    $database = 'file:'.$params['url'];
                    $connection = new LibSQL(
                        "libsql:dbname=$database",
                        LibSQL::OPEN_READWRITE | LibSQL::OPEN_CREATE,
                        $encryptionKey,
                    );
                    break;

                case 'memory':
                    $connection = new LibSQL(':memory:');
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
    private function getConnectionMode(array $params): string
    {
        $url = (string) ($params['url'] ?? '');
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
