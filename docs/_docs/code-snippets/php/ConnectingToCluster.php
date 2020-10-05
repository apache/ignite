<?php

require_once "/home/abudnikov/tmp/php-thin-client/vendor/autoload.php";

#tag::connecting[]
use Apache\Ignite\Client;
use Apache\Ignite\ClientConfiguration;
use Apache\Ignite\Exception\ClientException;

function connectClient(): void
{
    $client = new Client();
    try {
        $clientConfiguration = new ClientConfiguration(
            '127.0.0.1:10800', '127.0.0.1:10801', '127.0.0.1:10802');
        // Connect to Ignite node
        $client->connect($clientConfiguration);
    } catch (ClientException $e) {
        echo($e->getMessage());
    }
}

connectClient();
#end::connecting[]
