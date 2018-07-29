<?php

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Ignite\Client as Client;
use Apache\Ignite\ClientConfiguration as ClientConfiguration;
use Apache\Ignite\Exception\ClientException as ClientException;
use Apache\Ignite\CacheInterface as CacheInterface;

class Example
{
    const ENDPOINT = '127.0.0.1:10800';
    const CACHE_NAME = 'Example_cache';
    
    public function start()
    {
        $client = new Client();
        try {
            $client->connect(new ClientConfiguration(Example::ENDPOINT));

            $cache = $client->getOrCreateCache(Example::CACHE_NAME);

            $this->putGetData($cache);

            $client->destroyCache(Example::CACHE_NAME);
        } catch (ClientException $e) {
            echo('ERROR: ' . $e->getMessage());
        } finally {
            $client->disconnect();
        }
    }
    
    private function putGetData(CacheInterface $cache)
    {
        $keys = [1, 2, 3];
        foreach ($keys as $key) {
            $cache->put($key, $this->generateValue($key));
        }
        echo("Cache values put successfully\n");

        foreach ($keys as $key) {
            $value = $cache->get($key);
            if ($value !== $this->generateValue($key)) {
                echo("Unexpected cache value!\n");
                return;
            }
        }
        echo("Cache values get successfully\n");
    }
    
    private function generateValue(int $key): string
    {
        return 'value' . $key;
    }
}

$example = new Example();
$example->start();
