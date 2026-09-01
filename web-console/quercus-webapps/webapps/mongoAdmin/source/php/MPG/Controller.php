<?php

namespace MPG;

class Controller {

    /**
     * If it exists: returns request body.
     * 
     * @return string|null
     */
    private function getRequestBody() : ?string {

        $requestBody = file_get_contents('php://input');

        return is_string($requestBody) ? $requestBody : null;
        
    }

    /**
     * Returns request body, decoded.
     * 
     * 
     * @throws \Exception
     * 
     * @return array
     */
    public function getDecodedRequestBody() : array {

        $requestBody = $this->getRequestBody();

        if ( is_null($requestBody) ) {
            throw new \Exception('Request body is missing.');
        }

        $decodedRequestBody = json_decode($requestBody, JSON_OBJECT_AS_ARRAY);

        if ( is_null($decodedRequestBody) ) {
            throw new \Exception('Request body is invalid.');
        }

        return $decodedRequestBody;

    }
    
    
    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBCollection-insertOne/index.html
     */
    public static function normalizeInputJson(&$document) : JsonResponse {
    	
    	if ( isset($document['_id'])){
    		if (preg_match(MongoDBHelper::OBJECT_ID_REGEX, $document['_id']) ) {
    			// $document['_id'] =  \to_object_id($document['_id']);
    		}
    		elseif (preg_match(MongoDBHelper::UINT_REGEX, $document['_id']) ) {
    			$document['_id'] = intval($document['_id']);
    		}
    		
    	}
    	
    	
    	array_walk_recursive($document, function(&$documentValue) {
    		
    		if ( preg_match(MongoDBHelper::ISO_DATE_TIME_REGEX, $documentValue) ) {
    			$date = new \DateTime($documentValue);
    			$documentValue = new \Java('org.bson.BsonDateTime',$date->getTimestamp());
    		}
    		
    	});
    }
    
    public static function formatRecursively(&$document) {    	
    	if ( isset($document['_id']) && is_a($document['_id'], 'org.bson.types.Binary') && $document['_id']->type==4 ) {
    		$document['_id'] = \to_uuid($document['_id']);
    	}
    	
    	if ( isset($document['_id']) && is_a($document['_id'], 'org.bson.types.ObjectId') ) {
    		$document['_id'] = (string) $document['_id'];
    	}
    	
    	foreach ($document as $key=>&$documentValue) {
    		
    		if(is_a($documentValue, 'org.bson.types.Binary') && $documentValue->type==4 ) {
    			$document[$key] = \to_uuid($documentValue);
    		}    		
    		else if ( is_a($documentValue, '\MongoDB\Model\BSONArray')
    				|| is_a($documentValue, '\MongoDB\Model\BSONDocument') ) {
    			
    			self::formatRecursively($documentValue);
    					
    		} 
    		elseif ( is_a($documentValue, '\MongoDB\BSON\UTCDateTime') ) {
    			$documentValue = $documentValue->toDateTime()->format('Y-m-d\TH:i:s.v\Z');
    		}
    				
    	}
    	
    }

}
