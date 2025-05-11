<?php

namespace MPG;

use Limber\Router\Router;

Routes::setPrefix('/webapps/mongoAdmin/index.php?q=');

$router = new Router();

$router->get('/', function() {

    AuthController::ensureUserIsLogged();
    Routes::redirectTo('/queryDocuments');

});

$router->get(
    '/login',
    AuthController::class . '@login'
);

$router->post(
    '/login',
    AuthController::class . '@login'
);

$router->get(
    '/manageCollections',
    CollectionsController::class . '@manage'
);

$router->post(
    '/listCollections',
    CollectionsController::class . '@list'
);

$router->post(
    '/createCollection',
    CollectionsController::class . '@create'
);

$router->post(
    '/renameCollection',
    CollectionsController::class . '@rename'
);

$router->post(
    '/dropCollection',
    CollectionsController::class . '@drop'
);

$router->post(
	'/collStats',
	CollectionsController::class . '@collStats'
);

$router->get(
    '/importDocuments',
    DocumentsController::class . '@import'
);

$router->post(
    '/importDocuments',
    DocumentsController::class . '@import'
);

$router->get(
    '/visualizeDatabase',
    DatabasesController::class . '@visualize'
);

$router->get(
    '/getDatabaseGraph',
    DatabasesController::class . '@getGraph'
);

$router->get(
	'/queryDatabase',
	DatabasesController::class . '@query'
);
$router->post(
    '/aggregation',
	DatabasesController::class . '@aggregation'
);
$router->post(
	'/runCommand',
	DatabasesController::class . '@runCommand'
);
$router->post(
	'/dbstats',
	DatabasesController::class . '@dbstats'
);

$router->get(
    '/queryDocuments',
    DocumentsController::class . '@query'
);

$router->post(
    '/insertOneDocument',
    DocumentsController::class . '@insertOne'
);

$router->post(
    '/countDocuments',
    DocumentsController::class . '@count'
);

$router->post(
    '/deleteOneDocument',
    DocumentsController::class . '@deleteOne'
);

$router->post(
    '/convertSQLToMongoDBQuery',
    SQLController::class . '@convertToMongoDBQuery'
);

$router->post(
    '/findDocuments',
    DocumentsController::class . '@find'
);

$router->post(
    '/updateOneDocument',
    DocumentsController::class . '@updateOne'
);

$router->post(
    '/enumCollectionFields',
    CollectionsController::class . '@enumFields'
);

$router->get(
    '/manageIndexes',
    IndexesController::class . '@manage'
);

$router->post(
    '/createIndex',
    IndexesController::class . '@create'
);

$router->post(
    '/listIndexes',
    IndexesController::class . '@list'
);

$router->post(
    '/dropIndex',
    IndexesController::class . '@drop'
);

$router->get(
    '/manageUsers',
    UsersController::class . '@manage'
);

$router->post(
    '/createUser',
    UsersController::class . '@create'
);

$router->post(
    '/listUsers',
    UsersController::class . '@list'
);

$router->post(
    '/dropUser',
    UsersController::class . '@drop'
);

$router->get(
    '/logout',
    AuthController::class . '@logout'
);

return $router;
