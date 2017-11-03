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
 * To execute this script, run an Ignite instance with 'redis-ignite-internal-cache-0' cache specified and configured.
 * You will also need to have Predis extension installed. See https://github.com/nrk/predis for Predis details.
 *
 * See https://apacheignite.readme.io/docs/redis for more details on Redis integration.
 */

// Load the library.
require 'predis/autoload.php';
Predis\Autoloader::register();

// Connect.
try {
    $redis = new Predis\Client(array(
        "host" => "localhost",
        "port" => 11211));

    echo ">>> Successfully connected to Redis. \n";

    // Put entry to cache.
    if ($redis->set('k1', '1'))
        echo ">>> Successfully put entry in cache. \n";

    // Check entry value.
    echo(">>> Value for 'k1': " . $redis->get('k1') . "\n");

    // Change entry's value.
    if ($redis->set('k1', 'new_value'))
        echo ">>> Successfully put entry in cache. \n";

    // Check entry value.
    echo(">>> Value for 'k1': " . $redis->get('k1') . "\n");

    // Put entry to cache.
    if ($redis->set('k2', '2'))
        echo ">>> Successfully put entry in cache. \n";

    // Check entry value.
    echo(">>> Value for 'k2': " . $redis->get('k2') . "\n");

    // Get two entries.
    $val = $redis->mget('k1', 'k2');
    echo(">>> Value for 'k1' and 'k2': " . var_dump($val) . "\n");

    // Delete on entry.
    if ($redis->del('k1'))
        echo ">>> Successfully deleted 'k1'. \n";

    // Db size.
    echo ">>> Db size: " . $redis->dbsize() . "\n";

    // Increment.
    echo ">>> Incremented: " . $redis->incr("inc_k") . "\n";

    // Increment by 5.
    echo ">>> Incremented: " . $redis->incrby("inc_k", 5) . "\n";
}
catch (Exception $e) {
    echo ">>> Couldn't connected to Redis.";
    echo $e->getMessage();
}
?>
