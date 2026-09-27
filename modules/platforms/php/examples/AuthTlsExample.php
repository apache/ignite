<?php
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Ignite\Client;
use Apache\Ignite\ClientConfiguration;
use Apache\Ignite\Cache\CacheInterface;
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Type\ObjectType;

// This example demonstrates how to establish a secure connection to an Ignite node and use username/password authentication,
// as well as basic Key-Value Queries operations for primitive types:
// - connects to a node using TLS and providing username/password
// - creates a cache, if it doesn't exist
//   - specifies key and value type of the cache
// - put data of primitive types into the cache
// - get data from the cache
// - destroys the cache
class AuthTlsExample
{
    const ENDPOINT = 'localhost:10800';
    const USER_NAME = 'ignite';
    const PASSWORD = 'ignite';

    const TLS_CLIENT_CERT_FILE_NAME = __DIR__ . '/certs/client.pem';
    const TLS_CA_FILE_NAME = __DIR__ . '/certs/ca.pem';

    const CACHE_NAME = 'AuthTlsExample_cache';

    public function start(): void
    {
        $client = new Client();
        try {
            $tlsOptions = [
                'local_cert' => AuthTlsExample::TLS_CLIENT_CERT_FILE_NAME,
                'cafile' => AuthTlsExample::TLS_CA_FILE_NAME
            ];
            
            $config = (new ClientConfiguration(AuthTlsExample::ENDPOINT))->
                setUserName(AuthTlsExample::USER_NAME)->
                setPassword(AuthTlsExample::PASSWORD)->
                setTLSOptions($tlsOptions);
                    
            $client->connect($config);

            echo("Client connected successfully (with TLS and authentication enabled)" . PHP_EOL);

            $cache = $client->getOrCreateCache(AuthTlsExample::CACHE_NAME)->
                setKeyType(ObjectType::BYTE)->
                setValueType(ObjectType::SHORT_ARRAY);

            $this->putGetData($cache);
            $client->destroyCache(AuthTlsExample::CACHE_NAME);
        } catch (ClientException $e) {
            echo('ERROR: ' . $e->getMessage() . PHP_EOL);
        } finally {
            $client->disconnect();
        }
    }

    private function putGetData(CacheInterface $cache): void
    {
        $values = [
            1 => $this->generateValue(1),
            2 => $this->generateValue(2),
            3 => $this->generateValue(3)
        ];

        // put values
        foreach ($values as $key => $value) {
            $cache->put($key, $value);
        }
        echo('Cache values put successfully:' . PHP_EOL);
        foreach ($values as $key => $value) {
            $this->printValue($key, $value);
        }

        // get and compare values
        echo('Cache values get:' . PHP_EOL);
        foreach ($values as $key => $value) {
            $cacheValue = $cache->get($key);
            $this->printValue($key, $cacheValue);
            if (!$this->compareValues($value, $cacheValue)) {
                echo('Unexpected cache value!' . PHP_EOL);
                return;
            }
        }
        echo('Cache values compared successfully' . PHP_EOL);
    }

    private function compareValues(array $array1, array $array2): bool
    {
        return count(array_diff($array1, $array2)) === 0;
    }

    private function generateValue(int $key): array
    {
        $length = $key + 2;
        $result = [];
        for ($i = 0; $i < $length; $i++) {
            array_push($result, $key * 10 + $i);
        }
        return $result;
    }

    private function printValue($key, $value): void
    {
        echo(sprintf('  %d => [%s]%s', $key, implode(', ', $value), PHP_EOL));
    }
}

$authTlsExample = new AuthTlsExample();
$authTlsExample->start();
