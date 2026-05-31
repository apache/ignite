<?php

namespace MPG;

class Routes {

    private static $prefix;    

    public static function setPrefix($prefix_sep = NULL) {
    	// If request matches a folder. For example: /mongo/
    	if ( preg_match('#/$#', $_SERVER['REQUEST_URI']) ) {
    		
    		$prefix = $_SERVER['REQUEST_URI'];
    		
    	} else {
    		
    		$prefix = dirname($_SERVER['REQUEST_URI']);
    		
    		// Normalize directory separator in request path.
    		if ( DIRECTORY_SEPARATOR !== '/' ) {
    			$prefix = str_replace(DIRECTORY_SEPARATOR, '/', $prefix);
    		}
    		
    	}
    	
    	if(empty($prefix_sep)){
    		self::$prefix = rtrim($prefix, '/');
    	}
    	else{
    		self::$prefix = rtrim($prefix_sep, '/');
    		
    	}        

    }

    /**
     * Returns routes prefix, without trailing slash.
     * Example: /mongo
     */
    public static function getPrefix() : string {
        return self::$prefix;
    }

    /**
     * Redirects to a route.
     * 
     * @param string $route Route with leading slash.
     * Example: /queryDocuments
     */
    public static function redirectTo(string $route) {

        header('Location: ' . self::$prefix . $route);
        exit;

    }

}
