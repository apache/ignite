<?php

namespace MPG;

use Capsule\Response;

class JsonResponse extends Response {

    public function __construct(int $statusCode, $body, array $headers = []) {

        $headers = array_merge($headers, ['Content-Type' => 'application/json']);

        if(is_object($body)){
        	$body = json_encode($body);
        }        
        else if(is_array($body)){
        	$body = json_encode($body);
        }
        else if(is_bool($body)){
        	$body = json_encode($body);
        }
        
        return parent::__construct($statusCode, $body, $headers);
        
    }

}
