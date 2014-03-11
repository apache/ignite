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
 * To execute this script simply start up an instance of GridGain with cache enabled.
 * You can use any cache configuration from examples/config folder as follows:
 * ----
 * ${GRIDGAIN_HOME}/bin/ggstart.sh examples/config/example-cache.xml
 * ----
 *
 * Make sure you have correctly specified $CACHE_NAME script global variable
 * accordingly to your configuration. In mentioned above example-cache.xml we
 * have 3 caches configured. The names are the following: 'partitioned',
 * 'replicated' and 'local'. We have no default cache in this configuration.
 */

// Make sure CURL is present.
if (!function_exists('curl_init'))
    die('CURL not supported. (introduced in PHP 4.0.2)');

// Make sure json_decode is present (PHP 5.2.0+).
if (!function_exists('json_decode'))
    die('JSON not supported. (introduced in PHP 5.2.0)');

$URL = 'http://localhost:8080/gridgain?';

// Cache name to use.
// Null or empty string for default cache.
$CACHE_NAME = "partitioned";

/**
 * Creates URL parameters.
 *
 * @param $map URL parameters.
 * @return void
 */
function makeUrlParams($map) {
    $urlParams = "";

    foreach($map as $key => $value) $urlParams .= $key . '=' . urlencode($value) . '&';

    return rtrim($urlParams, '& ');
}

/**
 * Sets REST command over HTTP.
 *
 * @param $query Query parameters.
 * @param $post Post data.
 * @return void
 */
function execute($query, $post) {
    global $URL;

    $cmd = $query['cmd'];

    $query = makeUrlParams($query);
    $post = makeUrlParams($post);

    $api = $URL . $query;

    echo "Executing command '$cmd' with url '$api' ...\n\n";

    // Initiate curl object.
    $request = curl_init($api);

    curl_setopt($request, CURLOPT_HEADER, 0); // Set to 0 to eliminate header info from response.
    curl_setopt($request, CURLOPT_RETURNTRANSFER, 1); // Returns response data instead of TRUE(1).
    curl_setopt($request, CURLOPT_POSTFIELDS, $post); // Use HTTP POST to send form data.

    // Uncomment if you get no gateway response and are using HTTPS.
    // curl_setopt($request, CURLOPT_SSL_VERIFYPEER, FALSE);

    // Execute curl post and store results in $response.
    $response = (string)curl_exec($request);

    // Close curl object.
    curl_close($request);

    if (!$response)
        die('Nothing was returned. Do you have a connection to GridGain Jetty server?');

    echo "Response received from GridGain: $response\n\n";

    // JSON decoder.
    $result = json_decode($response, true);

    // The entire result printed out.
    echo "JSON result parsed into array:\n";
    print_r($result);

    echo str_repeat("-", 20) . "\n\n";
}

$rand = rand(1, 1000);

$key = "mykey-" . $rand; // Cache key.
$val = "myval-" . $rand; // Cache value.
$clientId = "2128506-ad94-4a21-a711-" . $rand; // Client id.

// Get command (will not return data, as cache is empty).
execute(array('cmd' => 'get', 'key' => $key, 'clientId' => $clientId, 'cacheName' => $CACHE_NAME), array());

// Put command.
execute(array('cmd' => 'put', 'key' => $key, 'clientId' => $clientId, 'cacheName' => $CACHE_NAME), array('val' => $val));

// Get command (will return value 'myval' stored in cache by 'put' command above).
execute(array('cmd' => 'get', 'key' => $key, 'clientId' => $clientId, 'cacheName' => $CACHE_NAME), array());

$rand = rand(1, 1000);

$key1 = "mykey-" . $rand; // Cache key.
$val1 = "myval-" . $rand; // Cache value.

$rand = rand(1, 1000);

$key2 = "mykey-" . $rand; // Cache key.
$val2 = "myval-" . $rand; // Cache value.

// Put all command.
execute(array('cmd' => 'putall', 'k1' => $key1, 'k2' => $key2, 'clientId' => $clientId, 'cacheName' => $CACHE_NAME),
        array('v1' => $val1, 'v2' => $val2));

// Get all command.
execute(array('cmd' => 'getall', 'k1' => $key1, 'k2' => $key2, 'clientId' => $clientId, 'cacheName' => $CACHE_NAME), array());
?>
