<?php

namespace MPG;



/**
 * 
 * TODO: Split this helper into many helpers.
 */
class MongoDBHelper {

    /**
     * Regular expression for a MongoDB URI.
     * 
     * @var string
     */
    public const URI_REGEX = '/^mongodb(\+srv)?:\/\/.+$/';

    /**
     * Regular expression for a MongoDB ObjectID.
     * 
     * @var string
     * @see https://stackoverflow.com/questions/20988446/regex-for-mongodb-objectid
     */
    public const OBJECT_ID_REGEX = '/^[a-f\d]{24}$/i';

    /**
     * Regular expression for an unsigned integer.
     * 
     * @var string
     */
    public const UINT_REGEX = '/^(0|[1-9][0-9]*)$/';

    /**
     * Regular expression for an ISO date-time.
     * 
     * @var string
     * @see https://stackoverflow.com/questions/3143070/javascript-regex-iso-datetime
     */
    public const ISO_DATE_TIME_REGEX = '/\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z)/';

    /**
     * Regular expression for a regular expression.
     * 
     * @var string
     */
    public const REGEX = '#^/(.+)/([igmsuy]*)$#';

    /**
     * MongoDB client singleton instance.
     * 
     * @var null|MongoDB\Client|MongoClient
     */
    private static $client;

    /**
     * Creates a MongoDB client.
     * 
     * @throws \Exception
     * @return MongoDB\Client
     */
    private static function createClient() : Client {

        if ( !isset($_SESSION['mpg']['user_is_logged']) ) {
            throw new \Exception('Session expired. Refresh browser.');
        }

        if ( isset($_SESSION['mpg']['mongodb_uri']) ) {

            $clientUri = $_SESSION['mpg']['mongodb_uri'];

        } else {
            
            $clientUri = 'mongodb://';

            if ( isset($_SESSION['mpg']['mongodb_user'])
                && isset($_SESSION['mpg']['mongodb_password'])
            ) {
                $clientUri .= rawurlencode($_SESSION['mpg']['mongodb_user']) . ':';
                $clientUri .= rawurlencode($_SESSION['mpg']['mongodb_password']) . '@';
                
                
            }
    
            $clientUri .= $_SESSION['mpg']['mongodb_host'];
    
            if ( isset($_SESSION['mpg']['mongodb_port']) ) {
                $clientUri .= ':' . $_SESSION['mpg']['mongodb_port'];
            }
            // When it's not defined: port defaults to 27017.
    
            if ( isset($_SESSION['mpg']['mongodb_database']) ) {
                $clientUri .= '/' . $_SESSION['mpg']['mongodb_database'];
            }
            
            $clientUri .= '/?ssl=false&maxPoolSize=5&retryReads=false';
            
            if(isset($_SESSION['mpg']['mongodb_user'])){
            	$clientUri .= '&authMechanism=PLAIN'; // &authMechanism=PLAIN
            }

        }
        
        if (function_exists('java') ){
        	$mongo_class = java_class('com.mongodb.client.MongoClients');
        	
        	$mongo = $mongo_class->create($clientUri);
        	
        	return $mongo;
        }

        return new MongoDBClient($clientUri);

    }

    /**
     * Gets MongoBD client singleton instance.
     * 
     * @return MongoDB\Client
     */
    public static function getClient() : MongoClient {

        if ( is_null(self::$client) ) {
            self::$client = self::createClient();  
        }

        return self::$client;

    }
    
    
    /**
     * Gets MongoBD client singleton instance.
     *
     * @return MongoDB\Client
     */
    public static function getCollection(string $db, string $collection) : MongoCollection {
    	
    	if ( is_null(self::$client) ) {
    		self::$client = self::createClient();
    	}
    	
    	return self::$client->getDatabase($db)->getCollection($collection);
    	
    }

    /**
     * Creates a MongoDB Regex from a string.
     * 
     * @throws \Exception
     * @return \MongoDB\BSON\Regex
     */
    public static function createRegexFromString(string $regexAsString) : BsonRegularExpression {

        $regexParts = [];

        if ( !preg_match(self::REGEX, $regexAsString, $regexParts) ) {
            throw new \Exception($regexAsString . ' is not a regular expression.');
        }

        $regexPattern = $regexParts[1];
        $regexFlags = $regexParts[2];
        
        if (function_exists('java')){
        	return new Java('org.bson.BsonRegularExpression',$regexPattern,$regexFlags);
        }

        return new \MongoDB\BSON\Regex($regexPattern, $regexFlags);

    }

}
