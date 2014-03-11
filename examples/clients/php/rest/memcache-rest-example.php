#!/bin/php

<?php
/* @php.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

/**
 * This example shows how to use PHP Memcache client for manipulating GridGain cache.
 *
 * To execute this script you need to have PHP Memcached extension installed.
 * See http://pecl.php.net/package/memcached for details.
 *
 * You can use default cache configuration from examples/config folder to
 * start up an instance of GridGain with cache enabled as follows:
 * ----
 * ${GRIDGAIN_HOME}/bin/ggstart.sh examples/config/example-cache-default.xml
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
