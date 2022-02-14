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
require_once "/home/abudnikov/tmp/php-thin-client/vendor/autoload.php";

use Apache\Ignite\Client;
use Apache\Ignite\ClientConfiguration;

//tag::tls[]
$tlsOptions = [
    'local_cert' => '/path/to/client/cert',
    'cafile' => '/path/to/ca/file',
    'local_pk' => '/path/to/key/file'
];

$config = new ClientConfiguration('localhost:10800');
$config->setTLSOptions($tlsOptions);

$client = new Client();
$client->connect($config);
//end::tls[]

//tag::authentication[]
$config = new ClientConfiguration('localhost:10800');
$config->setUserName('ignite');
$config->setPassword('ignite');
//$config->setTLSOptions($tlsOptions);

$client = new Client();
$client->connect($config);
//end::authentication[]
