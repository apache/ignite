<?php

namespace MPG;

class IndexesController extends Controller {

    public function manage() : ViewResponse {

        AuthController::ensureUserIsLogged();

        return new ViewResponse(200, 'manageIndexes', [
            'databaseNames' => DatabasesController::getDatabaseNames()
        ]);

    }

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBCollection-createIndex/index.html
     */
    public function create() : JsonResponse {

        try {
            $decodedRequestBody = $this->getDecodedRequestBody();
        } catch (\Throwable $th) {
            return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
        }

        try {

            $collection = MongoDBHelper::getCollection(
                $decodedRequestBody['databaseName'], $decodedRequestBody['collectionName']
            );
            
            $indexOptions = new \Java('com.mongodb.client.model.IndexOptions');
            $options = $decodedRequestBody['options'];
            if(isset($options['name'])){
            	$indexOptions->name($options['name']);
            }
            if(isset($options['unique'])){
            	$indexOptions->unique($options['unique']);
            }
            if(isset($options['sparse'])){
            	$indexOptions->sparse($options['sparse']);
            }

            $createdIndexName = $collection->createIndex(
            		$decodedRequestBody['key'], $indexOptions
            );

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, $createdIndexName);

    }

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBCollection-listIndexes/index.html
     */
    public function list() : JsonResponse {

        try {
            $decodedRequestBody = $this->getDecodedRequestBody();
        } catch (\Throwable $th) {
            return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
        }

        $indexes = [];

        try {

            $collection = MongoDBHelper::getCollection(
                $decodedRequestBody['databaseName'], $decodedRequestBody['collectionName']
            );

            foreach ($collection->listIndexes() as $indexInfo) {
                $indexes[] = [
                    'name' => $indexInfo['name'],
                    'keys' => $indexInfo['key'],
                    'isUnique' => $indexInfo['isUnique']
                ];
            }

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, $indexes);

    }

    /**
     * @see https://docs.mongodb.com/php-library/v1.12/reference/method/MongoDBCollection-dropIndex/index.html
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

            // TODO: Check dropIndex result?
            $collection->dropIndex($decodedRequestBody['indexName']);

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, true);

    }

}
