<?php

namespace MPG;

class UsersController extends Controller {

    public function manage() : ViewResponse {

        AuthController::ensureUserIsLogged();
        
        return new ViewResponse(200, 'manageUsers', [
            'databaseNames' => DatabasesController::getDatabaseNames()
        ]);

    }

    /**
     * @see https://docs.mongodb.com/manual/reference/command/createUser/
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

            // TODO: Check createUser result?
            $usersInfo = $database->runCommand([
                'createUser' => $decodedRequestBody['userName'],
                'pwd' => $decodedRequestBody['userPassword'],
                'roles' => $decodedRequestBody['userRoles']
            ]);

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, true);

    }

    /**
     * @see https://docs.mongodb.com/manual/reference/command/usersInfo/
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

            $usersInfoCommandResult = $database->runCommand(['usersInfo' => 1]);
            $usersInfo = $usersInfoCommandResult;

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, $usersInfo);

    }

    /**
     * @see https://docs.mongodb.com/manual/reference/command/dropUser/
     */
    public function drop() : JsonResponse {

        try {
            $decodedRequestBody = $this->getDecodedRequestBody();
        } catch (\Throwable $th) {
            return new JsonResponse(400, ErrorNormalizer::normalize($th, __METHOD__));
        }

        try {

            $database = MongoDBHelper::getClient()->getDatabase(
                $decodedRequestBody['databaseName']
            );

            // TODO: Check dropUser result?
            $usersInfo = $database->runCommand(['dropUser' => $decodedRequestBody['userName']]);

        } catch (\Throwable $th) {
            return new JsonResponse(500, ErrorNormalizer::normalize($th, __METHOD__));
        }

        return new JsonResponse(200, true);

    }

}
