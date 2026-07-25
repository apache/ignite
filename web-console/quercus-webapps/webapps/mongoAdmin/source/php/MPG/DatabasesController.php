<?php

namespace MPG;
use Capsule\Response;

class DatabasesController extends Controller {

    public static function getDatabaseNames() : array {

        $databaseNames = [];

        if ( isset($_SESSION['mpg']['mongodb_database']) ) {
            $databaseNames[] = $_SESSION['mpg']['mongodb_database'];
        } else {

            try {
                foreach (MongoDBHelper::getClient()->listDatabases() as $databaseInfo) {
                    $databaseNames[] = $databaseInfo['name'];
                }
            } catch (\Throwable $th) {
                ErrorNormalizer::prettyPrintAndDie($th);
            }

        }

        sort($databaseNames);

        return $databaseNames;

    }
    
    public function query() : ViewResponse {
    	
    	AuthController::ensureUserIsLogged();
    	
    	return new ViewResponse(200, 'queryDatabase', [
    			'databaseNames' => DatabasesController::getDatabaseNames()
    	]);
    	
    }
    
    
    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/aggregation/index.html
     */
    public function runCommand() : JsonResponse {
    	
    	try {
    		$decodedRequestBody = $this->getDecodedRequestBody();
    	} catch (\Throwable $th) {
    		return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
    	}
    	
    	self::normalizeInputJson($decodedRequestBody['filter']);
    	
    	try {
    		
    		foreach ($decodedRequestBody['filter'] as &$filterValue) {
    			if ( is_string($filterValue) && preg_match(MongoDBHelper::REGEX, $filterValue) ) {
    				$filterValue = MongoDBHelper::createRegexFromString($filterValue);
    			}
    		}
    		
    		$database = MongoDBHelper::getClient()->getDatabase($decodedRequestBody['databaseName']);
    		
    		$options = $decodedRequestBody['options'];
    		$documents = $database->runCommand(to_document($decodedRequestBody['filter']));
    		
    		
    	} catch (\Throwable $th) {
    		return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
    	}    	
    	return new JsonResponse(200, $documents);
    	
    }
    
    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/aggregation/index.html
     */
    public function aggregation() : JsonResponse {
    	
    	try {
    		$decodedRequestBody = $this->getDecodedRequestBody();
    	} catch (\Throwable $th) {
    		return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
    	}
    	
    	self::normalizeInputJson($decodedRequestBody['filter']);
    	
    	try {
    		
    		foreach ($decodedRequestBody['filter'] as &$filterValue) {
    			if ( is_string($filterValue) && preg_match(MongoDBHelper::REGEX, $filterValue) ) {
    				$filterValue = MongoDBHelper::createRegexFromString($filterValue);
    			}
    		}
    		if(empty($decodedRequestBody['collectionName'])){
    			$database = MongoDBHelper::getClient()->getDatabase($decodedRequestBody['databaseName']);
    		}
    		else{
    			$database = MongoDBHelper::getCollection($decodedRequestBody['databaseName'],$decodedRequestBody['collectionName']);
    		}
    		
    		$options = $decodedRequestBody['options'];
    		$find = $database->aggregate(to_document_list($decodedRequestBody['filter']));
    		if(isset($options['explain'])){
    			$find = $find->explain();
    		}
    		if(isset($options['maxTime'])){
    			$find = $find->maxTime($options['maxTime']);
    		}
    		if(isset($options['sort'])){
    			$find = $find->sort($options['sort']);
    		}
    		$documents = $find;
    		
    	} catch (\Throwable $th) {
    		return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
    	}
    	$result = [];
    	foreach ($documents as $document) {
    		self::formatRecursively($document);
    		$result[] = $document; #->toJson();   
    	}
    	
    	return new JsonResponse(200, $result);
    	
    }
    
    

    public function visualize() : ViewResponse {

        AuthController::ensureUserIsLogged();
        
        $serverStatus = $this->serverStatus();
        
        return new ViewResponse(200, 'visualizeDatabase',['serverStatus'=>$serverStatus->getBody()]);

    }

    public function getGraph() : JsonResponse {
    	
        $networkGraph = [
            'visData' => [
                'nodes' => [
                    [
                        'id' => 1,
                        'label' => 'MongoDB server',
                        'shape' => 'image',
                        'image' => './assets/images/leaf-icon.svg',
                        'size' => 32
                    ]
                ],
                'edges' => []
            ],
            'mapping' => [

                1 => [
                    'databaseName' => null,
                    'collectionName' => null
                ]

            ]
        ];

        $nodeCounter = 1;

        try {

            foreach (self::getDatabaseNames() as $databaseName) {

                $nodeCounter++;

                $databaseNode = [
                    'id' => $nodeCounter,
                    'label' => 'DB: ' . $databaseName,
                    'shape' => 'image',
                    'image' => './assets/images/database-icon.svg',
                    'size' => 24
                ];

                $database = MongoDBHelper::getClient()->getDatabase($databaseName);
    
                foreach ($database->listCollections() as $collectionInfo) {

                    $nodeCounter++;
                    
                    $collectionNode = [
                        'id' => $nodeCounter,
                        'label' => $collectionInfo['name'],
                        'shape' => 'image',
                        'image' => './assets/images/document-icon.svg',
                        'size' => 24
                    ];

                    array_push($networkGraph['visData']['nodes'], $collectionNode);

                    array_push($networkGraph['visData']['edges'], [
                        'from' => $databaseNode['id'],
                        'to' => $collectionNode['id']
                    ]);

                    $networkGraph['mapping'][ $collectionNode['id'] ] = [
                        'databaseName' => $databaseName,
                        'collectionName' => $collectionInfo['name']
                    ];

                }
                
                array_push($networkGraph['visData']['nodes'], $databaseNode);

                array_push($networkGraph['visData']['edges'], [
                    'from' => 1, // MongoDB server
                    'to' => $databaseNode['id']
                ]);

                $networkGraph['mapping'][ $databaseNode['id'] ] = [
                    'databaseName' => $databaseName,
                    'collectionName' => null
                ];

            }

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, $networkGraph);

    }
    
    /**
     * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
     */
    public function serverStatus() : JsonResponse {
    	
    	try {
    		$decodedRequestBody = $this->getDecodedRequestBody();
    	} catch (\Throwable $th) {
    		return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
    	}
    	
    	try {
    		$db = $decodedRequestBody && isset($decodedRequestBody['databaseName']) ? $decodedRequestBody['databaseName'] : 'admin';
    		$database = MongoDBHelper::getClient()->getDatabase($db);
    		
    		// TODO: Check serverStatus result?
    		$serverStatus = $database->runCommand([
    			'serverStatus' => 1,
    		]);
    		
    	} catch (\Throwable $th) {
    		return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
    	}
    	
    	require_once ABS_PATH. '/source/php/smarty_filter/smarty_filter.php';
    	
    	$serverStatusView = smarty_filter_process('stats/servers.tpl',['info'=>$serverStatus]);
    	
    	return new Response(200, $serverStatusView);
    	
    }
    
    /**
     * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
     */
    public function dbstats() : Response {
    	
    	try {
    		$decodedRequestBody = $this->getDecodedRequestBody();
    	} catch (\Throwable $th) {
    		return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
    	}
    	
    	try {
    		
    		$database = MongoDBHelper::getClient()->getDatabase($decodedRequestBody['databaseName']);
    		
    		// TODO: Check serverStatus result?
    		$serverStatus = $database->runCommand([
    				'dbstats' => 1,
    		]);
    		
    	} catch (\Throwable $th) {
    		return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
    	}
    	
    	require_once ABS_PATH. '/source/php/smarty_filter/smarty_filter.php';
    	
    	$dbStats = smarty_filter_process('stats/dbs.tpl',['stats'=>$serverStatus]);
    	
    	return new Response(200, $dbStats);
    	
    }

}
