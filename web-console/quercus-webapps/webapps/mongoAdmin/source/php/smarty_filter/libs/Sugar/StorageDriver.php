<?php
/**
 * Sugar storage interface.
 *
 * This is an interface used for defining custom storage drivers.  Storage
 * drivers are responsible for loading template files.  An application might
 * want a custom driver for loadng templates from a database, for example.
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
 * @version    SVN: $Id: StorageDriver.php 324 2010-08-29 09:53:05Z Sean.Middleditch $
 * @link       http://php-sugar.net
 */

/**
 * Storage driver interface.
 *
 * Interface for storage drivers.  These are used to load template from
 * different resources, such as the file system or a database.
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
interface Sugar_StorageDriver
{
    /**
     * Search for the template and return a driver-specific handle.
     *
     * Given a reference to a template, look up the template and return
     * a drive specific handle.  This handle may be anything, including
     * an object, a string, an array, or an integer.  The value FALSE
     * is returned to indicate that the template does not exist or
     * cannot be found.
     *
     * The File storage driver, as an example, simply returns the full
     * path to the template, or FALSE if the template cannot be found.
     *
     * The handle will be passed to the other storage driver methods
     * for loading data or checking timestamps. 
     *
     * Note that, depending on the driver, it may be possible for a
     * template to be found when getHandle() is called, and thus have
     * a valid handle, but then be deleted or removed before an other 
     * methods are called.  Fault-tolerant drivers should be prepared.
     *
     * @param string $name Name of the template to look up.
     *
     * @return mixed Driver-specific handle, or FALSE if the template
     *               cannot be found.
     */
    public function getHandle($name);

    /**
     * Returns the timestamp of the handle.
     *
     * @param mixed $handle Handle returned by getHandle().
     *
     * @return int Timestamp if it exists, or zero if it cannot be found.
     */
    public function getLastModified($handle);

    /**
     * Returns the source for the handle.
     *
     * @param mixed $handle Handle to lookup.
     *
     * @return string Source of handle.
     */
    public function getSource($handle);

    /**
     * Returns a user-friendly name for the handle.
     *
     * The purpose of this function is to provide the most
     * user-friendly name for a handle that is possible.  If
     * in doubt, simply return the $name parameter.
     *
     * For an example, the File driver returns the $handle
     * parameter, as this is the full path of the template.
     * The full path is the most user-friendly name, as it
     * identifies where the specific template is.  The name
     * alone may be ambiguous if there are multiple template
     * directories.
     *
     * The returned value should help the user get to the
     * template easily and quickly.  It may be useful for
     * some drivers in CMS systems to return a URL to the
     * edit page for the requested template, for instance.
     *
     * @param mixed  $handle Handle returned by getHandle().
     * @param string $name   Name of the template requested.
     *
     * @return string User-friendly name for the handle.
     */
    public function getName($handle, $name);
}
// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
