<?php
/**
 * Exceptions used by Sugar.
 *
 * Various exception classes used by Sugar.
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
 * @subpackage Exceptions
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    SVN: $Id: Exception.php 302 2010-04-12 03:56:06Z Sean.Middleditch $
 * @link       http://php-sugar.net
 */

/**
 * Generic Sugar exception.
 *
 * This is the base class for any exception thrown by Sugar.  It
 * is provided to make it easier to catch all Sugar errors.
 *
 * @category   Template
 * @package    Sugar
 * @subpackage Exceptions
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    Release: 0.83
 * @link       http://php-sugar.net
 */
class Sugar_Exception extends Exception
{
    /**
     * Constructor.
     *
     * @param string $msg Error message.
     */
    public function __construct($msg)
    {
        parent::__construct($msg);
    }
}

/**
 * Parse error.
 *
 * Thrown during the template parsing stage when the template contains
 * invalid Sugar template markup that cannot be processed.  This
 * is a fatal, non-recoverable error inside the parser.
 *
 * @category   Template
 * @package    Sugar
 * @subpackage Exceptions
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    Release: 0.83
 * @link       http://php-sugar.net
 */
class Sugar_Exception_Parse extends Sugar_Exception
{
    /**
     * File error occured in.
     *
     * @var string $file
     */
    public $file = '<input>';

    /**
     * Line error occured in.
     *
     * @var int $line
     */
    public $line = 1;

    /**
     * Constructor.
     *
     * @param string $file File the error occured in.
     * @param int    $line Line the error occured in.
     * @param string $msg  Error message.
     */
    public function __construct($file, $line, $msg)
    {
        parent::__construct('parse error at '.$file.','.$line.': '.$msg);
        $this->file = $file;
        $this->line = $line;
    }
}

/**
 * Runtime error.
 *
 * Thrown for any errors that occur during execution of a compiled
 * template.  These errors cannot be recovered from during execution.
 *
 * @category   Template
 * @package    Sugar
 * @subpackage Exceptions
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    Release: 0.83
 * @link       http://php-sugar.net
 */
class Sugar_Exception_Runtime extends Sugar_Exception
{
    /**
     * File error occured in.
     *
     * @var string $file
     */
    public $file = '<input>';

    /**
     * Line error occured in.
     *
     * @var int $line
     */
    public $line = 1;

    /**
     * Constructor.
     *
     * @param string $file File the error occured in.
     * @param int    $line Line the error occured in.
     * @param string $msg  Error message.
     */
    public function __construct($file, $line, $msg)
    {
        parent::__construct('runtime error at '.$file.','.$line.': '.$msg);
        $this->file = $file;
        $this->line = $line;
    }
}

/**
 * Invocation error.
 *
 * Thrown whenever the user has mis-used the Sugar API.
 *
 * @category   Template
 * @package    Sugar
 * @subpackage Exceptions
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    Release: 0.83
 * @link       http://php-sugar.net
 */
class Sugar_Exception_Usage extends Sugar_Exception
{
    /**
     * Constructor.
     *
     * @param string $msg Error message.
     */
    public function __construct($msg)
    {
        parent::__construct('api misuse error: '.$msg);
    }
}

// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
