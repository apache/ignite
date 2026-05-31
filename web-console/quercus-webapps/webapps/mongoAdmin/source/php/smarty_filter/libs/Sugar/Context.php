<?php
/**
 * Class for managing variable contexts
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
 * @copyright  2010 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    SVN: $Id: Lexer.php 320 2010-08-25 08:15:44Z Sean.Middleditch $
 * @link       http://php-sugar.net
 * @access     private
 */

/**
 * Variable context
 *
 * Keeps track of the hierarchal contexts (scopes) variables are defined in.
 *
 * @category   Template
 * @package    Sugar
 * @subpackage Runtime
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2010 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    Release: 0.83
 * @link       http://php-sugar.net
 * @access     private
 */
class Sugar_Context
{
    /**
     * Parent context, if any
     *
     * @var Sugar_Context $_parent
     */
    private $_parent;

    /**
     * Variables
     *
     * @var array $_vars
     */
    private $_vars;

    /**
     * Create instance
     *
     * @param mixed $parent Optional parent
     * @param array $vars   Vars for context
     */
    public function __construct($parent, array $vars = array())
    {
        $this->_parent = $parent;
        $this->_vars = $vars;
    }

    /**
     * Get a variable value by name
     *
     * @param string $name Name of variable to lookup
     *
     * @return mixed Variable value, null if not found.
     */
    public function get($name)
    {
        $name = strtolower($name);

        // iterate through parent stack (avoid recursion overhead)
        $context = $this;
        do {
            if (isset($context->_vars[$name])) {
                return $context->_vars[$name];
            } else {
                $context = $context->_parent;
            }
        } while ($context);
        return null;
    }

    /**
     * (Re)assign a variable's value
     *
     * @param string $name  Name of variable to assign
     * @param mixed  $value Value of variable
     */
    public function set($name, $value)
    {
        $name = strtolower($name);
        $this->_vars [$name]= $value;
    }
}
// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
