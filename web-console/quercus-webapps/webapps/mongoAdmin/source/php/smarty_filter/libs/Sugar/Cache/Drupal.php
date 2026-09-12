<?php
/**
 * Drupal-based cache driver for Sugar
 *
 */

/**
 * Drupal-based cache driver.
 *
 * Uses {$link Sugar::$cacheTime} to control behavior.
 *
 * @category   Template
 * @package    Sugar
 * @subpackage Drivers
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    Release: 0.83
 * @link       http://php-sugar.net
 */
class Sugar_Cache_Drupal implements Sugar_CacheDriver
{
    /**
     * Sugar instance.
     *
     * @var Sugar $_sugar
     */
    private $_sugar;

    /**
     * Instance of Memcached.
     *
     * @var Memcached $_memcached;
     */
    private $_cache_name;

    /**
     * Constructor.
     *
     * @param Sugar $sugar Sugar instance.
     */
    public function __construct($sugar,$cache_name='cache_filter')
    {
        $this->_sugar = $sugar;
        $this->_cache_name = $cache_name;
    }

    /**
     * Makes a key for the given reference.
     *
     * @param Sugar_Template $template  File reference.
     * @param string         $type Either 'ctpl' or 'chtml'.
     *
     * @return string Key.
     */
    private function _makeKey(Sugar_Template $template, $type)
    {
        $key = $template->name;
        if ($type == Sugar::CACHE_HTML && !is_null($template->cacheId)) {
            $key .= '^'.$template->cacheId;
        }
        $key .= '^'.$type;
        return $key;
    }

    /**
     * Returns the timestamp.
     *
     * @param Sugar_Template $template  File reference.
     * @param string         $type Either 'ctpl' or 'chtml'.
     *
     * @return int Timestamp
     */
    public function getLastModified(Sugar_Template $template, $type)
    {
        // fetch the item from the cache
        $key = $this->_makeKey($template, $type);
        $item = cache_get($key,$this->_cache_name);

        if ($item !== FALSE) {
            return $item->created;
        } else {
            return false;
        }
    }

    /**
     * Returns the bytecode for the requested reference.
     *
     * @param Sugar_Template $template  File reference to lookup.
     * @param string         $type Either 'ctpl' or 'chtml'.
     *
     * @return array Bytecode, or false if not in the cache.
     */
    public function load(Sugar_Template $template, $type)
    {
        // fetch the item from the cache
        $key = $this->_makeKey($template, $type);
        $item = cache_get($key,$this->_cache_name);

        if ($item !== FALSE) {
            return $item->data;
        } else {
            return false;
        }
    }

    /**
     * Adds the bytecode to the cache.
     *
     * @param Sugar_Template $template  File reference to lookup.
     * @param string         $type Either 'ctpl' or 'chtml'.
     * @param array          $data Bytecode.
     *
     * @return bool True on success.
     * @throws Sugar_Exception_Usage when the cache directory is missing or
     * otherwise unusable.
     */
    public function store(Sugar_Template $template, $type, $data)
    {
        $key = $this->_makeKey($template, $type);
        cache_set($key, $data, $this->_cache_name,CACHE_TEMPORARY);
        return true;
    }

    /**
     * Erases the bytecode for the requested reference.
     *
     * @param Sugar_Template $template  File reference for the bytecode to erase.
     * @param string         $type Either 'ctpl' or 'chtml'.
     *
     * @return bool True on success.
     */
    public function erase(Sugar_Template $template, $type)
    {
        $key = $this->_makeKey($template, $type);
        cache_clear_all($key,$this->_cache_name);
    }

    /**
     * Clears all caches the driver is responsible for.
     *
     * @return bool True on success.
     */
    public function clear()
    {
        cache_clear_all(NULL,$this->_cache_name);
        return true;
    }
}
// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
