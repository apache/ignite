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
 * To execute this script you will have to enable optional `ignite-rest-http` module -
 * copy `libs/optional/ignite-rest-http` folder into `libs` (one level up).
 *
 * After that start up an instance of Ignite with cache enabled.
 * You can use configuration from examples/config folder as follows:
 * ----
 * ${IGNITE_HOME}/bin/ignite.sh examples/config/example-cache.xml
 * ----
 *
 * Make sure you have correctly specified $CACHE_NAME script global variable
 * accordingly to your configuration. In mentioned above example-cache.xml we
 * have only default cache configured.
 */

// Make sure CURL is present.
if (!function_exists('curl_init'))
    die('CURL not supported. (introduced in PHP 4.0.2)');

// Make sure json_decode is present (PHP 5.2.0+).
if (!function_exists('json_decode'))
    die('JSON not supported. (introduced in PHP 5.2.0)');

$URL = 'http://localhost:8080/ignite?';

// Cache name to use.
$CACHE_NAME = 'default';

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
        die('Nothing was returned. Do you have a connection to Ignite Jetty server?');

    echo "Response received from Ignite: $response\n\n";

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
