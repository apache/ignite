<?php
/**
 * Miscellaneous utility functions used by Sugar.
 *
 * Provides several utility functions used by the Sugar codebase.
 *
 * PHP version 5
 *
 * LICENSE:
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @category  Template
 * @package   Sugar
 * @author    Sean Middleditch <sean@mojodo.com>
 * @copyright 2008-2009 Mojodo, Inc. and contributors
 * @license   http://opensource.org/licenses/mit-license.php MIT
 * @version   SVN: $Id: Util.php 338 2010-09-11 20:04:23Z Sean.Middleditch $
 * @link      http://php-sugar.net
 */

/**
 * Returns an argument from a function parameter list, supporting both
 * position and named parameters and default values.
 *
 * @param array  $params  Function parameter list.
 * @param string $name    Parameter name.
 * @param mixed  $default Default value if parameter is not specified.
 *
 * @return mixed Value of parameter if given, or the default value otherwise.
 */
function Sugar_Util_GetArg($params, $name, $default = null)
{
    return isset($params[$name]) ? $params[$name] : $default;
}

/**
 * Checks if an array is a "vector," or an array with only integral
 * indexes starting at zero and incrementally increasing.  Used only
 * for nice exporting to JavaScript.
 *
 * Only really used for {@link Sugar_Util_Json}.
 *
 * @param array $array Array to check.
 *
 * @return bool True if array is a vector.
 */
function Sugar_Util_IsVector($array)
{
    if (!is_array($array)) {
        return false;
    }

    $next = 0;
    foreach ($array as $k=>$v) {
        if ($k !== $next) {
            return false;
        }
        ++$next;
    }
    return true;
}

/**
 * Performs string-escaping for JavaScript values.
 *
 * This is the equivalent of running addslashes() and then replacing
 * control characters with their escaped equivalents.
 *
 * @param mixed $string String to escape.
 *
 * @return string Formatted result.
 */
function Sugar_Util_EscapeJavascript($string) {
    $escaped = addslashes($string);
    $escaped = str_replace(array("\n", "\r", "\r\n"), '\\n', $escaped);
    return $escaped;
}

/**
 * Formats a PHP value in JavaScript format.
 *
 * We can probably juse use json_encode() instead of this, except
 * json_encode() is PHP 5.2 only.
 *
 * @param mixed $value Value to format.
 *
 * @return string Formatted result.
 */
function Sugar_Util_Json($value)
{
    // use json_encode, if defined
    if (function_exists('json_encode')) {
        return json_encode($value);
    }

    switch (gettype($value)) {
    case 'integer':
    case 'float':
        return $value;
    case 'array':
        if (Sugar_Util_IsVector($value)) {
            $escaped = array_map('Sugar_Util_Json', $value);
            return '['.implode(',', $escaped).']';
        }

        $result = '{';
        $first = true;
        foreach ($value as $k=>$v) {
            if (!$first) {
                $result .= ',';
            } else {
                $first = false;
            }
            $result .= Sugar_Util_Json($k).':'.Sugar_Util_Json($v);
        }
        $result .= '}';
        return $result;
    case 'object':
        $result = '{\'phpType\':'.Sugar_Util_Json(get_class($value));
        foreach (get_object_vars($value) as $k=>$v) {
            $result .= ',' . Sugar_Util_Json($k).':'.Sugar_Util_Json($v);
        }
        $result .= '}';
        return $result;
    case 'null':
        return 'null';
    default:
        $escaped = Sugar_Util_EscapeJavascript($value);
        return '"'.$escaped.'"';
    }
}

/**
 * Convert a value into a timestamp.  This is essentially strtotime(),
 * except that if an integer timestamp is passed in it is returned
 * verbatim, and if the value cannot be parsed, it returns the current
 * timestamp.
 *
 * @param mixed $value Time value to parse.
 *
 * @return int Timestamp.
 */
function Sugar_Util_ValueToTime($value)
{
    if (is_int($value)) {
        // raw int?  it's a timestamp
        return $value;
    } elseif (is_string($value)) {
        // otherwise, convert it with strtotime
        return strtotime($value);
    } else {
        // something... use current time
        return time();
    }
}

/**
 * Attempt to locate a file in one or more source directories.
 *
 * @param mixed  $haystack The directory/directories to search in.
 * @param string $needle   The file being searched for.
 *
 * @return mixed The full path to the file if found, false otherwise.
 */
function Sugar_Util_SearchForFile($haystack, $needle)
{
    // search multiple directories if templateDir is an array,
    // otherwise only search the given dir
    if (is_array($haystack)) {
        foreach ($haystack as $dir) {
            $path = $dir.'/'.$needle;
            if (is_file($path) && is_readable($path)) {
                return $path;
            }
        }
    } else {
        $path = $haystack.'/'.$needle;
        if (is_file($path) && is_readable($path)) {
            return $path;
        }
    }
    
    // no matches found
    return false;
}

// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
