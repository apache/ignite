<?php

namespace MPG;
use Capsule\Response;

class CollectionsController extends Controller {

    public function manage() : ViewResponse {

        AuthController::ensureUserIsLogged();
        
        return new ViewResponse(200, 'manageCollections', [
            'databaseNames' => DatabasesController::getDatabaseNames(),
        	'collectionStats' => ''
        ]);

    }

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBDatabase-listCollections/index.html
     */
    public function list() : JsonResponse {

        try {
            $decodedRequestBody = $this->getDecodedRequestBody();
        } catch (\Throwable $th) {
            return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
        }

        try {

            $database = MongoDBHelper::getClient()->getDatabase(
                $decodedRequestBody['databaseName']
            );

            $collectionNames = [];

            foreach ($database->listCollections() as $collectionInfo) {
                $collectionNames[] = $collectionInfo['name'];
            }

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        sort($collectionNames);

        return new JsonResponse(200, $collectionNames);

    }

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBDatabase-createCollection/index.html
     */
    public function create() : JsonResponse {

        try {
            $decodedRequestBody = $this->getDecodedRequestBody();
        } catch (\Throwable $th) {
            return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
        }

        try {

            $database = MongoDBHelper::getClient()->getDatabase(
                $decodedRequestBody['databaseName']
            );

            // TODO: Check createCollection result?
            $database->createCollection($decodedRequestBody['collectionName']);

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }
        
        return new JsonResponse(200, true);

    }

    /**
     * @see https://docs.mongodb.com/manual/reference/command/renameCollection/
     */
    public function rename() : JsonResponse {

        try {
            $decodedRequestBody = $this->getDecodedRequestBody();
        } catch (\Throwable $th) {
            return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
        }

        try {

            $database = MongoDBHelper::getClient()->getDatabase('admin');

            // TODO: Check command result?
            $database->runCommand([
                'renameCollection' => $decodedRequestBody['databaseName'] . '.'
                    . $decodedRequestBody['oldCollectionName'],
                'to' => $decodedRequestBody['databaseName'] . '.'
                    . $decodedRequestBody['newCollectionName']
            ]);

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, true);

    }

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBCollection-drop/index.html
     */
    public function drop() : JsonResponse {

        try {
            $decodedRequestBody = $this->getDecodedRequestBody();
        } catch (\Throwable $th) {
            return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
        }

        try {

            $collection = MongoDBHelper::getCollection(
                $decodedRequestBody['databaseName'], $decodedRequestBody['collectionName']
            );

            // TODO: Check drop result?
            $collection->drop();

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, true);

    }

    public function enumFields() : JsonResponse {

        try {
            $decodedRequestBody = $this->getDecodedRequestBody();
        } catch (\Throwable $th) {
            return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
        }

        try {

            $collection = MongoDBHelper::getCollection(
                $decodedRequestBody['databaseName'], $decodedRequestBody['collectionName']
            );

            $documents = $collection->find()->limit(2);

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        if ( empty($documents) ) {
            return new JsonResponse(200, []);
        }

        $document = $documents[0];

        if (isset($document['_id']) && is_a($document['_id'], 'org.bson.types.ObjectId')) {
            $document['_id'] = (string) $document['_id'];
        }

        DocumentsController::formatRecursively($document);

        $array = json_decode(json_encode($document), JSON_OBJECT_AS_ARRAY);

        /**
         * Converts multidimensional array to 2D array with dot notation keys.
         * @see https://stackoverflow.com/questions/10424335/php-convert-multidimensional-array-to-2d-array-with-dot-notation-keys
         */
        $ritit = new \RecursiveIteratorIterator(new \RecursiveArrayIterator($array));
        $result = [];

        foreach ($ritit as $unusedValue) {
            $keys = [];
            foreach (range(0, $ritit->getDepth()) as $depth) {
                $keys[] = $ritit->getSubIterator($depth)->key();
            }
            $result[] = join('.', $keys);
        }

        $documentFields = array_unique($result);

        return new JsonResponse(200, $documentFields);

    }
    
    /**
     * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
     */
    public function collStats() : JsonResponse {
    	
    	try {
    		$decodedRequestBody = $this->getDecodedRequestBody();
    	} catch (\Throwable $th) {
    		return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
    	}
    	
    	if(empty($decodedRequestBody['databaseName']) || empty($decodedRequestBody['collectionName'])){
    		return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
    	}
    	
    	try {
    		$db = $decodedRequestBody['databaseName'];
    		$database = MongoDBHelper::getClient()->getDatabase($db);
    		
    		// TODO: Check serverStatus result?
    		$collStats = $database->runCommand([
    			'collStats' => $decodedRequestBody['collectionName'], 				
    		]);
    		
    	} catch (\Throwable $th) {
    		return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
    	}
    	
    	require_once ABS_PATH. '/source/php/smarty_filter/smarty_filter.php';
    	
    	$collectionStats = smarty_filter_process('stats/colls.tpl',['stats'=>$collStats]);
    	
    	return new Response(200, $collectionStats);
    	
    }

}
