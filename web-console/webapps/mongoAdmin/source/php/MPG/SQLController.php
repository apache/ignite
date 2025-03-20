<?php

namespace MPG;

class SQLController extends Controller {
	
	public function convertToMongoDBQuery() : JsonResponse {
		
		try {
			$decodedRequestBody = $this->getDecodedRequestBody();
		} catch (\Throwable $th) {
			return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
		}
		
		// Converter takes only inline SQL as input.
		$decodedRequestBody['query'] = str_replace("\n", ' ', $decodedRequestBody['sql']);
				
		$mongoDBQuery = json_encode(['$sql'=>$decodedRequestBody['query']]);		
		
		return new JsonResponse(200, $mongoDBQuery);
		
	}
	
	

    public function convertToMongoDBQueryByJavaProcess() : JsonResponse {

        try {
            $decodedRequestBody = $this->getDecodedRequestBody();
        } catch (\Throwable $th) {
            return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
        }

        // Converter takes only inline SQL as input.
        $decodedRequestBody['sql'] = str_replace("\n", ' ', $decodedRequestBody['sql']);

        $decodedRequestBody['sql'] = str_replace('"', '\"', $decodedRequestBody['sql']);

        $jarPath = '"' . ABS_PATH 
            . '/extras/programs/sql-to-mongo-db-query-converter-1.13-standalone.jar"';
        $jarArgs = '--sql "' . $decodedRequestBody['sql'] . '"';
        
        $command = 'java -jar ' . $jarPath . ' ' . $jarArgs;

        if ( strtoupper(substr(PHP_OS, 0, 3)) === 'WIN' ) {
            // Force output.
            $command .= ' 2>&1';
        }

        $commandResult = str_replace("\n", '', shell_exec($command));
        
        $mongoDBQuery = [];

        try {
            if ( !preg_match_all('|^.*\.find\({(.*)}\)$|s', $commandResult, $mongoDBQuery) ) {
                throw new \Exception(
                    'Impossible to convert (SELECT) SQL query to MongoDB query... ' . 
                    'Try to install Java JDK on the computer hosting "MongoDB PHP GUI".'
                );
            }
        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, '{' . $mongoDBQuery[1][0] . '}');

    }
    
}
