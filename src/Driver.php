<?php

declare(strict_types=1);

namespace Turso\Doctrine\DBAL;

use Doctrine\DBAL\Driver\AbstractSQLiteDriver;
use LibSQL;
use SensitiveParameter;

final class Driver extends AbstractSQLiteDriver
{
    private LibSQL $connection;
    protected bool $isStandAlone = true;

    public function connect(
        #[SensitiveParameter]
        array $params,
    ): Connection {

        if (
            isset($params['driverOptions']['use_framework']) &&
            isset($params['driverOptions']['url']) &&
            isset($params['driverOptions']['auth_token']) &&
            isset($params['driverOptions']['sync_url'])
        ) {
            $params['url']          = str_replace('sqlite:///', '', $params['driverOptions']['url']);
            $params['auth_token']   = $params['driverOptions']['auth_token'];
            $params['sync_url']     = $params['driverOptions']['sync_url'];
            $this->isStandAlone     = false;
        } else if (
            isset($params['driverOptions']['use_framework']) &&
            !isset($params['driverOptions']['url']) &&
            isset($params['driverOptions']['auth_token']) &&
            isset($params['driverOptions']['sync_url'])
        ) {
            $params['url']          = null;
            $params['auth_token']   = $params['driverOptions']['auth_token'];
            $params['sync_url']     = $params['driverOptions']['sync_url'];
            $this->isStandAlone     = false;
        } else if (
            isset($params['driverOptions']['use_framework']) &&
            isset($params['path'])
        ) {
            $params['url']          = str_replace('sqlite:///', '', $params['path']);
            $this->isStandAlone     = false;
        } else if (
            isset($params['driverOptions']['use_framework']) &&
            isset($params['memory'])
        ) {
            $params['url']          = ':memory:';
            $this->isStandAlone     = false;
        }

        try {
            switch ($this->getConnectionMode($params)) {
                case 'remote_replica':
                    $defaultParams = [
                        "sync_interval"     => 5,
                        "read_your_writes"  => true,
                        "encryption_key"    => ""
                    ];

                    $params['url'] = "file:" . $params['url'];

                    $config = \array_merge($params, $defaultParams);

                    $databaseConfig = [
                        "url"               => $config['url'],
                        "authToken"         => $config['auth_token'],
                        "syncUrl"           => $config['sync_url'],
                        "syncInterval"      => $config['sync_interval'],
                        "read_your_writes"  => $config['read_your_writes'],
                        "encryptionKey"     => $config['encryption_key']
                    ];
                    $this->connection = new LibSQL($databaseConfig);
                    break;
                case 'remote':
                    $this->connection = new LibSQL("libsql:dbname={$params['sync_url']};authToken={$params['auth_token']}");
                    break;
                case 'local':
                    $encryption_key = !empty($params['encryption_key']) ? $params['encryption_key'] : "";
                    $database = "file:" . $params['url'];
                    $this->connection = new LibSQL("libsql:dbname=$database", LibSQL::OPEN_READWRITE | LibSQL::OPEN_CREATE, $encryption_key);
                    break;
                case 'memory':
                    $this->connection = new LibSQL(":memory:");
                    break;

                default:
                    throw new \Exception("Connection mode is not found");
                    break;
            }
        } catch (\Exception $e) {
            throw Exception::new($e);
        }

        return new Connection($this->connection, $this->isStandAlone);
    }

    private function getConnectionMode($params): string
    {
        if (
            (isset($params['url']) && $this->in_strpos($params['url'], ['.db', '.sqlite']) !== false) &&
            !empty($params['auth_token']) &&
            !empty($params['sync_url'])
        ) {
            return "remote_replica";
        } else if (
            !empty($params['auth_token']) &&
            !empty($params['sync_url'])
        ) {
            return "remote";
        } else if ($this->in_strpos($params['url'], ['.db', '.sqlite']) !== false) {
            return "local";
        } else {
            return "memory";
        }
    }

    private function in_strpos(string $haystack, array $needle): bool
    {
        foreach ($needle as $substring) {
            if (strpos($haystack, $substring) !== false) {
                return true;
            }
        }
        return false;
    }
}
