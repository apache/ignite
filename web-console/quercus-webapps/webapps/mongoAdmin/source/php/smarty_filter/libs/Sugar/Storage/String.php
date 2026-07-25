<?php
/**
 * String-based storage driver.
 *
 * Allows loading a template from a user string.
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
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    SVN: $Id: String.php 319 2010-08-25 07:56:29Z Sean.Middleditch $
 * @link       http://php-sugar.net
 */

/**
 * String-based storage driver.
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
class Sugar_Storage_String implements Sugar_StorageDriver
{
    /**
     * Sugar instances.
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
     * Returns the source code (the name) as the handle.
     *
     * @param string $name Name (source) of the template to load.
     *
     * @return string The value of $name.
     */
    public function getHandle($name)
    {
        return $name;
    }

    /**
     * Returns the current time.
     *
     * @param string $handle Handle for the template.
     *
     * @return int current time.
     */
    public function getLastModified($handle)
    {
        return time();
    }

    /**
     * Returns the source of the template.
     *
     * @param string $handle Template source.
     *
     * @return string Source of the template.
     */
    public function getSource($handle)
    {
        return $handle;
    }

    /**
     * Returns the text 'source template'.
     *
     * @param string $handle Path to the template.
     * @param string $name   Name of the template.
     *
     * @return string 'source template'.
     */
    public function getName($handle, $name)
    {
        return 'source template'; 
    }
}
// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
