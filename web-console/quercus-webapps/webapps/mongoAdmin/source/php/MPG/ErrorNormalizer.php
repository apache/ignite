<?php

namespace MPG;

class ErrorNormalizer {

    /**
     * Normalizes an error.
     * 
     * @param \Throwable $error
     * @param ?string $function
     * 
     * @return array
     */
    public static function normalize(\Throwable $error, string $function = null) : array {

        $normalizedError = ['error' => null];

        $normalizedError['error']['code'] = $error->getCode();
        $normalizedError['error']['message'] = $error->getMessage();

        if ( !is_null($function) ) {
            $normalizedError['error']['function'] = $function;
        }

        return $normalizedError;

    }

    /**
     * Normalizes then prints an error prettily then terminates script.
     * 
     * @param \Throwable $error
     */
    public static function prettyPrintAndDie(\Throwable $error) {

        http_response_code(500);
        header('Content-Type: application/json');

        echo json_encode(self::normalize($error), JSON_PRETTY_PRINT);

        die;

    }

}
