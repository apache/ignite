<?php

namespace MPG;

class DocumentsController extends Controller {    
    

    public function import() : ViewResponse {

        AuthController::ensureUserIsLogged();

        $successMessage = '';
        $errorMessage = '';

        if ( isset($_FILES['import']) && isset($_FILES['import']['tmp_name'])
            && isset($_POST['database_name']) && isset($_POST['collection_name']) ) {

                try {
					$id_field = $_POST['mpg-id-field-input'];
					$skip = $_POST['mpg-skip-input'];
					$limit = $_POST['mpg-limit-input'];
                    $importedDocumentsCount = self::importFromFile(
                    	$_FILES['import']['name'],
                        $_FILES['import']['tmp_name'],
                        $_POST['database_name'],
                        $_POST['collection_name'],
                    	$id_field,$skip,$limit
                    );

                    $successMessage = $importedDocumentsCount . ' document(s) imported in '; 
                    $successMessage .= $_POST['collection_name'] . '.';

                } catch (\Throwable $th) {
                    $errorMessage = $th->getMessage();
                }

        }

        return new ViewResponse(200, 'importDocuments', [
            'databaseNames' => DatabasesController::getDatabaseNames(),
            'maxFileSize' => ini_get('upload_max_filesize'),
            'successMessage' => $successMessage,
            'errorMessage' => $errorMessage
        ]);

    }

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBCollection-insertMany/
     */
    private static function importFromFile($documentsFilename, $filepath, $databaseName, $collectionName, $idFiled=NULL, $skip=0, $limit=-1) : int {
    	$json_documents = []; // 用于存储解码后的JSON对象
    	$fileInfo = pathinfo($documentsFilename);
    	
    	if($fileInfo['extension']=='json'){
    		
    		$documentsFileContents = @file_get_contents($filepath);
	
	        if ( $documentsFileContents === false ) {
	            throw new \Exception('Impossible to read the import file.');
	        }
	
	        // Remove UTF-8 BOM from uploaded file since UTF-8 BOM can disturb decoding of JSON.
	        $documentsFileContents = preg_replace('/\x{FEFF}/u', '', $documentsFileContents, 1);
	        
	        $json_documents = json_decode($documentsFileContents, JSON_OBJECT_AS_ARRAY);
	
	        if ( is_null($json_documents) ) {
	            throw new \Exception('Import file is invalid... Malformed JSON?');
	        }
    	}
    	else if($fileInfo['extension']=='jsonl'){    		
    		
    		// 打开文件进行读取
    		$handle = fopen($filepath, 'r');
    		if ($handle) {
    			// 逐行读取文件内容
    			while (($line = fgets($handle)) !== false) {
    				// 去除每行末尾的换行符（如果有的话）
    				$line = trim($line);
    				
    				// 对每一行进行JSON解码
    				$jsonObject = json_decode($line, JSON_OBJECT_AS_ARRAY); // 第二个参数true表示将JSON对象转换为关联数组
    				
    				// 检查解码是否成功
    				if (json_last_error() === JSON_ERROR_NONE) {
    					// 将解码后的JSON对象添加到数组中
    					$json_documents[] = $jsonObject;
    				} else {
    					// 处理解码错误（可选）
    					throw new \Exception('JSON decode error on line: ' . $line . "\n");
    				}
    			}
    			
    			fclose($handle);
    			
    		} else {
    			// 处理文件打开失败的情况（可选）
    			throw new \Exception('Failed to open file: ' . $documentsFilename . "\n");
    		}
    	}
    	else {
    		$delimiter = ',';
    		if($fileInfo['extension']=='tsv'){
    			$delimiter = "\t"; // TSV文件的分隔符是制表符
    		}
    		$headers = []; // 用于存储表头
    		// 打开文件进行读取
    		$handle = fopen($filepath, 'r');
    		if ($handle) {
    			// 读取第一行作为表头
    			$headers = fgetcsv($handle,0,$delimiter);
    			if ($headers === false) {
    				
    				fclose($handle);
    				// 处理读取表头失败的情况
    				throw new \Exception("Failed to read headers from file: $documentsFilename");
    			}
    			
    			// 逐行读取数据部分
    			while (($row = fgetcsv($handle,0,$delimiter)) !== false) {
    				// $row是一个数组，包含了当前行的数据（不包括表头）
    				$doc = array_combine($headers, $row);
    				if(is_array($doc)){
    					$json_documents[] = $doc; // 使用表头作为键名
    				}
    			}    			
    			
    			fclose($handle);
    			
    		} else {
    			// 处理文件打开失败的情况
    			throw new \Exception('Failed to open file: ' . $documentsFilename . "\n");
    		}
    	}
    	
		$index=0;
		$documents = array();
		foreach ($json_documents as $document) {
        	if($skip>0 && $index<$skip){
        		$index++;
        		continue;
        	}
        	if($limit>0 && $index>=$limit+$skip){   		
        		break;
        	}
        	self::normalizeInputJson($document);
        	if($idFiled && isset($document[$idFiled])){
        		$document['_id'] = $document[$idFiled];
        	}
        	$documents[] = $document;
        	$index++;
        }

        $collection = MongoDBHelper::getCollection(
            $databaseName, $collectionName
        );

        $insertManyResult = $collection->insertMany(to_document_list($documents));

        return $insertManyResult->getInsertedIds()->size();

    }

    public function query() : ViewResponse {

        AuthController::ensureUserIsLogged();
        
        return new ViewResponse(200, 'queryDocuments', [
            'databaseNames' => DatabasesController::getDatabaseNames()
        ]);

    }

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBCollection-insertOne/index.html
     */
    public function insertOne() : JsonResponse {

        try {
            $decodedRequestBody = $this->getDecodedRequestBody();
        } catch (\Throwable $th) {
            return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
        }

        self::normalizeInputJson($decodedRequestBody['document']);
        

        try {

            $collection = MongoDBHelper::getCollection(
                $decodedRequestBody['databaseName'], $decodedRequestBody['collectionName']
            );

            $insertOneResult = $collection->insertOne(\to_document($decodedRequestBody['document']));

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, $insertOneResult->getInsertedCount());

    }

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBCollection-countDocuments/
     */
    public function count() : JsonResponse {

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

            $collection = MongoDBHelper::getCollection(
                $decodedRequestBody['databaseName'], $decodedRequestBody['collectionName']
            );

            $count = $collection->countDocuments($decodedRequestBody['filter']);

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, $count);

    }

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBCollection-deleteOne/index.html
     */
    public function deleteOne() : JsonResponse {

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

            $collection = MongoDBHelper::getCollection(
                $decodedRequestBody['databaseName'], $decodedRequestBody['collectionName']
            );

            $deleteOneResult = $collection->deleteOne($decodedRequestBody['filter']);

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, $deleteOneResult->getDeletedCount());

    }

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBCollection-find/index.html
     */
    public function find() : JsonResponse {

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

            $collection = MongoDBHelper::getCollection(
                $decodedRequestBody['databaseName'], $decodedRequestBody['collectionName']
            );

            $options = $decodedRequestBody['options'];
            $find = $collection->find($decodedRequestBody['filter']);
            if(isset($options['skip'])){
            	$find = $find->skip($options['skip']);
            }
            if(isset($options['limit'])){
            	$find = $find->limit($options['limit']);
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

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBCollection-updateOne/index.html
     */
    public function updateOne() : JsonResponse {

        try {
            $decodedRequestBody = $this->getDecodedRequestBody();
        } catch (\Throwable $th) {
            return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
        }
        
        self::normalizeInputJson($decodedRequestBody['filter']);

        self::normalizeInputJson($decodedRequestBody['update']['$set']);
       

        try {

            $collection = MongoDBHelper::getCollection(
                $decodedRequestBody['databaseName'], $decodedRequestBody['collectionName']
            );

            $updateResult = $collection->updateOne(
                $decodedRequestBody['filter'], to_document($decodedRequestBody['update'])
            );

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, $updateResult->getModifiedCount());

    }

}
