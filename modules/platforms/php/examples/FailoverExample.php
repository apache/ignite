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
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Exception\NoConnectionException;
use Apache\Ignite\Exception\OperationStatusUnknownException;

const ENDPOINT1 = 'localhost:10800';
const ENDPOINT2 = 'localhost:10801';
const ENDPOINT3 = 'localhost:10802';

const CACHE_NAME = 'test_cache';

// This example demonstrates failover behavior of the client
// - configures the client to connect to a set of nodes
// - connects to a node
// - executes an operation with Ignite server in a cycle (10 operations with 5 seconds pause) and finishes
// - if connection is broken, the client automatically tries to reconnect to another node
// - if not possible to connect to any nodes, the example finishes
function connectClient() {
    $client = new Client();
    $client->setDebug(true);
    try {
        $clientConfiguration = new ClientConfiguration(ENDPOINT1, ENDPOINT2, ENDPOINT3);
        // connect to Ignite node
        $client->connect($clientConfiguration);
        echo("Client connected successfully" . PHP_EOL);
        for ($i = 0; $i < 10; $i++) {
            try {
                $client->getOrCreateCache(CACHE_NAME);
                sleep(5);
            } catch (OperationStatusUnknownException $e) {
                // Status of the operation is unknown,
                // a real application may repeat the operation if necessary
            }
        }
    } catch (NoConnectionException $e) {
        // The client is disconnected, all further operation with Ignite server fail till the client is connected again.
        // A real application may recall $client->connect() with the same or different list of Ignite nodes.
        echo('ERROR: ' . $e->getMessage() . PHP_EOL);
    } catch (ClientException $e) {
        echo('ERROR: ' . $e->getMessage() . PHP_EOL);
    } finally {
        $client->disconnect();
    }
}

connectClient();
