<?php
/**
 * File-based cache driver for Sugar
 *
 * This class implements a file-based cache driver, which loads and saves
 * cache files in the $sugar->cacheDir directory.
 *
 *
 * @category   Template
 * @package    Sugar
 * @subpackage Drivers
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    SVN: $Id: File.php 328 2010-08-31 07:44:41Z Sean.Middleditch $
 * @link       http://php-sugar.net
 */

/**
 * File-based cache driver.
 *
 * Uses {@link Sugar::$cacheDir} and {$link Sugar::$cacheTime} to control
 * behavior.
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
class Sugar_Cache_File implements Sugar_CacheDriver
{
    /**
     * Sugar instance.
     *
     * @var Sugar $sugar
     */
    private $_sugar;

    /**
     * Constructor.
     *
     * @param Sugar $sugar Sugar instance.
     */
    public function __construct($sugar)
    {
        $this->_sugar = $sugar;
    }

    /**
     * Makes a path for the given reference.
     *
     * @param Sugar_Template $template  File reference.
     * @param string         $type Either 'ctpl' or 'chtml'.
     *
     * @return string Path.
     */
    private function _makePath(Sugar_Template $template, $type)
    {
        $path = $this->_sugar->cacheDir.'/';
        $path .= urlencode($template->name);
        if ($type == Sugar::CACHE_HTML && !is_null($template->cacheId)) {
            $path .= '^'.urlencode($template->cacheId);
        }
        $path .= '^'.$type;
        return $path;
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
        $path = $this->_makePath($template, $type);

        // check exists, return stamp
        if (file_exists($path) && is_file($path) && is_readable($path)
            && time() - filemtime($path) <= $this->_sugar->cacheLimit
        ) {
            return filemtime($path);
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
        $path = $this->_makePath($template, $type);
    
        // must exist, be readable, and not be older than $cacheLimit seconds
        if (file_exists($path) && is_file($path) && is_readable($path)
            && time() - filemtime($path) <= $this->_sugar->cacheLimit
        ) {
            // load, deserialize
            $data = file_get_contents($path);
            $data = unserialize($data);
            return $data;
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
        $path = $this->_makePath($template, $type);

        // ensure we can save the cache file
        if (!file_exists($this->_sugar->cacheDir)) {
            throw new Sugar_Exception(
                'cache directory does not exist: '.$this->_sugar->cacheDir
            );
        }
        if (!is_dir($this->_sugar->cacheDir)) {
            throw new Sugar_Exception(
                'cache directory is not a directory: '.$this->_sugar->cacheDir
            );
        }
        if (!is_writeable($this->_sugar->cacheDir)) {
            throw new Sugar_Exception(
                'cache directory is not writable: '.$this->_sugar->cacheDir
            );
        }

        // encode, save
        $data = serialize($data);
        file_put_contents($path, $data);
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
        $path = $this->_makePath($template, $type);

        // if the file exists and the directory is writeable, erase it
        if (file_exists($path)
            && is_file($path)
            && is_writeable($this->_sugar->cacheDir)
        ) {
            unlink($path);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Clears all caches the driver is responsible for.
     *
     * @return bool True on success.
     */
    public function clear()
    {
        // directory must exist, and be both readable and writable
        if (!file_exists($this->_sugar->cacheDir)
            || !is_dir($this->_sugar->cacheDir)
            || !is_writable($this->_sugar->cacheDir)
            || !is_readable($this->_sugar->cacheDir)
        ) {
            return false;
        }

        $dir = opendir($this->_sugar->cacheDir);
        while ($cache = readdir($dir)) {
            if (preg_match('/^[^.].*[.](ctpl|chtml)$/', $cache)) {
                unlink($this->_sugar->cacheDir.'/'.$cache);
            }
        }

        return true;
    }
}
// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
