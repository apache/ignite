<?php
/**
 * Memcache-based cache driver for Sugar
 *
 * This class implements a memcache-based cache driver, which loads and saves
 * cache files to a memcached server pool.
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
 * @category   Template
 * @package    Sugar
 * @subpackage Drivers
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2010 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    SVN: $Id: Memcache.php 305 2010-04-12 04:01:03Z Sean.Middleditch $
 * @link       http://php-sugar.net
 */

/**
 * Memcache-based cache driver.
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
class Sugar_Cache_Memcache implements Sugar_CacheDriver
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
    private $_memcached;

    /**
     * Constructor.
     *
     * @param Sugar $sugar Sugar instance.
     */
    public function __construct($sugar, Memcached $memcached)
    {
        $this->_sugar = $sugar;
        $this->_memcached = $memcached;
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
        $item = $this->memcached->get($key);

        if ($item !== FALSE) {
            return $item['stamp'];
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
        $item = $this->memcached->get($key);

        if ($item !== FALSE) {
            return $item['data'];
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
        $this->_memcached->set($key, array('stamp' => time(), 'data' => $data), $this->_sugar->cacheTime);
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
        $this->_memcached->delete($key);
    }

    /**
     * Clears all caches the driver is responsible for.
     *
     * @return bool True on success.
     */
    public function clear()
    {
        $this->_memcached->flush();
        return true;
    }
}
// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
