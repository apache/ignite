<?php

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
