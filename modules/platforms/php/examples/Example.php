<?php

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Ignite\IgniteClient as IgniteClient;
use Apache\Ignite\IgniteClientConfiguration as IgniteClientConfiguration;
use Apache\Ignite\Exception\IgniteClientException as IgniteClientException;
use Apache\Ignite\CacheClientInterface as CacheClientInterface;

class Example
{
    const ENDPOINT = '127.0.0.1:10800';
    const CACHE_NAME = 'Example_cache';
    
    public function start()
    {
        $igniteClient = new IgniteClient();
        try {
            $igniteClient->connect(new IgniteClientConfiguration(Example::ENDPOINT));

            $cache = $igniteClient->getOrCreateCache(Example::CACHE_NAME);

            $this->putGetData($cache);

            $igniteClient->destroyCache(Example::CACHE_NAME);
        }
        catch (IgniteClientException $e) {
            echo('ERROR: ' . $e->getMessage());
        }
        finally {
            $igniteClient->disconnect();
        }
    }
    
    private function putGetData(CacheClientInterface $cache)
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
