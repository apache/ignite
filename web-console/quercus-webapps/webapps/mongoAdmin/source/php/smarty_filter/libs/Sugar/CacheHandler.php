<?php
/**
 * Cache handler, a helper class for the runtime.
 *
 * This class is used to build up the cache output for cached runs of the
 * runtime engine.  It provides methods for storing cached output as well
 * as adding uncachable runtime code to be re-executed when the cache is
 * displayed.
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
 * @subpackage Runtime
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    SVN: $Id: CacheHandler.php 324 2010-08-29 09:53:05Z Sean.Middleditch $
 * @link       http://php-sugar.net
 * @access     private
 */

/**
 * Handlers the creation of Sugar caches, including non-cached bytecode
 * chunks.
 *
 * @category   Template
 * @package    Sugar
 * @subpackage Runtime
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    Release: 0.83
 * @link       http://php-sugar.net
 * @access     private
 */
class Sugar_CacheHandler
{
    /**
     * Sugar reference.
     *
     * @var Sugar
     */
    private $_sugar;

    /**
     * Text output.
     *
     * @var string
     */
    private $_output;

    /**
     * Bytecode result.
     *
     * @var array
     */
    private $_bc;

    /**
     * List of file references used, stored as strings.
     *
     * @var array
     */
    private $_refs;

    /**
     * Compresses the text output gathered so far onto the bytecode stack.
     *
     * @return bool True on success.
     */
    private function _compact()
    {
        if ($this->_output) {
            $this->_bc []= 'echo';
            $this->_bc []= $this->_output;
            $this->_output = '';
        }
        return true;
    }

    /**
     * Constructor.
     *
     * @param Sugar $sugar Sugar reference.
     */
    public function __construct($sugar)
    {
        $this->_sugar = $sugar;
    }

    /**
     * Adds text to the cache.
     *
     * @param string $text Text to append to cache.
     *
     * @return bool True on success.
     */
    public function addOutput($text)
    {
        $this->_output .= $text;
        return true;
    }

    /**
     * Adds a new file reference to the list of files
     * used in the template.
     *
     * @param Sugar_Template $template New reference.
     *
     * @return bool True on success.
     */
    public function addRef(Sugar_Template $template)
    {
        $this->_refs []= $template->name;
        return true;
    }

    /**
     * Adds bytecode to the cache.
     *
     * @param array $block Bytecode to append to cache.
     *
     * @return bool True on success.
     */
    public function addBlock($block)
    {
        $this->_compact();
        array_push($this->_bc, 'nocache', $block);
        return true;
    }

    /**
     * Returns the complete cache.
     *
     * @return array Cache.
     */
    public function getOutput()
    {
        $this->_compact();
        return array(
            'type' => 'chtml',
            'version' => Sugar::VERSION,
            'refs' => $this->_refs,
            'bytecode' => $this->_bc,
            'sections' => array()
        );
    }
}
// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
