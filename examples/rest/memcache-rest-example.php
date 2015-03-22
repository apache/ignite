#!/bin/php

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

/**
 * To execute this script you need to have PHP Memcached extension installed.
 * See http://pecl.php.net/package/memcached for details.
 *
 * To execute this script you will have to enable optional `ignite-rest-http` module -
 * copy `libs/optional/ignite-rest-http` folder into `libs` (one level up).
 *
 * After that start up an instance of Ignite with cache enabled.
 * You can use configuration from examples/config folder as follows:
 * ----
 * ${IGNITE_HOME}/bin/ignite.sh examples/config/example-cache.xml
 * ----
 */

// Create client instance.
$client = new Memcached();

// Set localhost and port (set to correct values).
$client->addServer("localhost", 11211);

// Force client to use binary protocol.
$client->setOption(Memcached::OPT_BINARY_PROTOCOL, true);

// Put entry to cache.
if ($client->add("key", "val"))
    echo ">>> Successfully put entry in cache.\n";

// Check entry value.
echo(">>> Value for 'key': " . $client->get("key") . "\n");

echo(">>>\n");

// Change value of entry.
if ($client->set("key", "newVal"))
    echo(">>> Successfully changed value of entry.\n");

// Check entry value.
echo(">>> New value for 'key': " . $client->get("key") . "\n");

echo(">>>\n");

// Put one more entry to cache.
if ($client->add("anotherKey", "anotherVal"))
    echo ">>> Successfully put entry in cache.\n";

// Check entry value.
echo(">>> Value for 'anotherKey': " . $client->get("anotherKey") . "\n");

echo(">>>\n");

// Get both entries.
$map = $client->getMulti(array("key", "anotherKey"));

if ($map) {
    echo(">>> Successfully fetched two entries from cache.\n");

    echo(">>> Value for 'key': " . $map["key"] . "\n");
    echo(">>> Value for 'anotherKey': " . $map["anotherKey"] . "\n");
}

echo(">>>\n");

// Remove one entry.
if ($client->delete("key"))
    echo(">>> Successfully removed entry from cache.\n");

echo(">>>\n");

// Remove all entries.
if ($client->flush())
    echo(">>> Successfully cleared cache.\n");

?>
