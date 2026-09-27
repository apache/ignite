<?php
/**
 * Runtime engine.
 *
 * This is the runtime engine, which takes the code compiled by SugarParser
 * and makes it go.  It handles the various operators, output escaping,
 * caching, and method/function invocation with exception safety nets.
 *
 * The design is not particularly clever or efficient, and could use a could
 * round of profiling and improvement.  Parsing only gets called once every
 * time a template is modified, but the runtime is invoked for every single
 * page display, even for cached pages.
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
 * @version    SVN: $Id: Runtime.php 349 2010-09-16 21:18:47Z Sean.Middleditch $
 * @link       http://php-sugar.net
 * @access     private
 */

/**
 * Sugar runtime engine.
 *
 * @category   Template
 * @package    Sugar
 * @subpackage Runtime
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    SVN: $Id: Runtime.php 349 2010-09-16 21:18:47Z Sean.Middleditch $
 * @link       http://php-sugar.net
 * @access     private
 */
class Sugar_Runtime {
    /**
     * Sugar handle
     *
     * @var public $sugar
     */
    public $sugar;

    /**
     * Constructor
     *
     * @param Sugar $sugar Sugar handle
     * @param array $vars  Defined variables
     */
    public function __construct(Sugar $sugar)
    {
        $this->sugar = $sugar;
    }

    /**
     * Converts a PHP value into something nice for a user to see.  Mainly
     * this is intended for arrays, objects, and boolean values, which are
     * not natively user-visible formats.
     *
     * @param mixed $value Value to convert.
     *
     * @return string User-visible rendition of the value.
     */
    private function _valueToString($value)
    {
        if (is_bool($value)) {
            return $value?'true':'false';
        } elseif (is_array($value)) {
            return Sugar_Util_Json($value);
        } else {
            return (string)$value;
        }
    }

    /**
     * Display output, either to the cache handler or to the PHP
     * output stream.
     *
     * @param string $output Output.
     *
     * @return bool True on success.
     */
    private function _display($output)
    {
        if ($this->sugar->cacheHandler) {
            return $this->sugar->cacheHandler->addOutput($output);
        } else {
            echo $output;
            return true;
        }
    }

    /**
     * Executes the given bytecode.  The return value is the last item on
     * the stack, if any.  For complete templates, this should be nothing
     * (null).
     *
     * @param Sugar_Context $vars     Variable context
     * @param array         $code     Bytecode to execute.
     * @param array         $sections Section bytecodes.
     *
     * @return mixed Last value on stack.
     * @throws Sugar_Exception_Runtime when the user has provided code that
     * cannot be executed, such as attempting to call a function that does
     * not exist.
     */
    public function execute(Sugar_Context $vars, $code, $sections)
    {
        $stack = array();

        for ($i = 0; $i < count($code); ++$i) {
            $opcode = $code[$i];
            switch($opcode) {
            case 'echo':
                $this->_display($code[++$i]);
                break;
            case 'print':
                $v1 = array_pop($stack);
                $this->_display($this->sugar->escape($this->_valueToString($v1)));
                break;
            case 'rawprint':
                $v1 = array_pop($stack);
                $this->_display($this->_valueToString($v1));
                break;
            case 'push':
                $v1 = $code[++$i];
                $stack []= $v1;
                break;
            case 'lookup':
                $name = strtolower($code[++$i]);
                $stack []= $vars->get($name);
                break;
            case 'assign':
                $name = $code[++$i];
                $v1 = array_pop($stack);
                $vars->set($name, $v1);
                break;
            case 'insert':
                $name = $code[++$i];
                if (isset($sections[$name])) {
                    $this->execute($vars, $sections[$name], $sections);
                } else {
                    throw new Sugar_Exception_Runtime(
                        $debug_file,
                        $debug_line,
                        'unknown section `'.$name.'`'
                    );
                }
                break;
            case 'negate':
                $v1 = array_pop($stack);
                $stack []= -$v1;
                break;
            case '!':
                $v1 = array_pop($stack);
                $stack []= !$v1;
                break;
            case '..':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= $v1 . $v2;
                break;
            case '+':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                if (is_numeric($v1) && is_numeric($v2)) {
                    $stack []= $v1 + $v2;
                } elseif (is_array($v1) && is_array($v2)) {
                    $stack []= array_merge($v1, $v2);
                } else {
                    $stack []= $v1 . $v2;
                }
                break;
            case '*':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= $v1 * $v2;
                break;
            case '-':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= $v1 - $v2;
                break;
            case '/':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                if ($v2 == 0) {
                    $stack []= null;
                } else {
                    $stack []= $v1 / $v2;
                }
                break;
            case '%':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                if ($v2 == 0) {
                    $stack []= null;
                } else {
                    $stack []= $v1 % $v2;
                }
                break;
            case '==':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= ($v1 == $v2);
                break;
            case '!=':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= ($v1 != $v2);
                break;
            case '||':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= ($v1 || $v2);
                break;
            case '&&':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= ($v1 && $v2);
                break;
            case '<':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= ($v1 < $v2);
                break;
            case '<=':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= ($v1 <= $v2);
                break;
            case '>':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= ($v1 > $v2);
                break;
            case '>=':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= ($v1 >= $v2);
                break;
            case 'in':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= (is_array($v2) && in_array($v1, $v2));
                break;
            case '!in':
                $v2 = array_pop($stack);
                $v1 = array_pop($stack);
                $stack []= (is_array($v2) && !in_array($v1, $v2));
                break;
            case 'call':
            case 'call_top':
                $func = $code[++$i];
                $args = $code[++$i];
                $escape_flag = $opcode == 'call_top' ? $code[++$i] : false;
                $debug_file = $code[++$i];
                $debug_line = $code[++$i];

                // lookup function
                $callable = $this->sugar->getFunction($func);
                if (!$callable) {
                    throw new Sugar_Exception_Runtime(
                        $debug_file,
                        $debug_line,
                        'unknown function `'.$func.'`'
                    );
                }

                // update escape flag based on function default
                $escape_flag = $escape_flag && $callable['escape'];

                // compile args
                $params = array();
                foreach ($args as $name=>$pcode) {
                    $params[$name] = $this->execute($vars, $pcode, $sections);
                }

                // exception net
                try {
                    // call function, using appropriate method
                    $ret = call_user_func($callable['invoke'], $this->sugar, $params);
                } catch (Exception $e) {
                    $this->sugar->handleError($e);
                    $ret = null;
                }

                // process return value
                if ($opcode == 'call_top' && $escape_flag) {
                    $this->_display($this->sugar->escape($this->_valueToString($ret)));
                } elseif ($opcode == 'call_top') {
                    $this->_display($this->_valueToString($ret));
                } else {
                    $stack []= $ret;
                }
                break;
            case 'method':
                $obj = array_pop($stack);
                $func = $code[++$i];
                $args = $code[++$i];
                $debug_file = $code[++$i];
                $debug_line = $code[++$i];

                // ensure the object is an object and that the method is a method
                if (!is_object($obj)) {
                    throw new Sugar_Exception_Runtime(
                        $debug_file,
                        $debug_line,
                        'method call on non-object type `'.gettype($obj).'`'
                    );
                }

                if (!method_exists($obj, $func)) {
                    throw new Sugar_Exception_Runtime(
                        $debug_file,
                        $debug_line,
                        'unknown method `'.$func.'` on type `'.gettype($obj).'`'
                    );
                }

                // compile args
                $params = array();
                foreach ($args as $pcode) {
                    $params [] = $this->execute($vars, $pcode, $sections);
                }

                // perform ACL checking on the method call
                if (!is_null($this->sugar->methodAcl)) {
                    $check = call_user_func(
                        $this->sugar->methodAcl,
                        $this->sugar,
                        $obj,
                        $func,
                        $params
                    );

                    if (!$check) {
                        throw new Sugar_Exception_Runtime(
                            $debug_file,
                            $debug_line,
                            'method call to `'.$func.'` on type `'.
                                gettype($obj).'` blocked by ACL'
                        );
                    }
                }

                // exception net
                try {
                    // invoke method
                    $stack []= @call_user_func_array(array($obj, $func), $params);
                } catch (Exception $e) {
                    $this->sugar->handleError($e);
                    $stack []= null;
                }
                break;
            case 'modifier':
                $name = $code[++$i];
                $args = $code[++$i];
                $value = array_pop($stack);

                // lookup function
                $callable = $this->sugar->getModifier($name);
                if (!$callable) {
                    throw new Sugar_Exception_Runtime(
                        'FIXME',
                        1,
                        'unknown modifier `'.$name.'`'
                    );
                }

                // compile args
                $params = array();
                foreach ($args as $pcode) {
                    $params []= $this->execute($vars, $pcode, $sections);
                }

                // exception net
                try {
                    // invoke the modifier
                    $ret = call_user_func($callable, $value, $this->sugar, $params);
                } catch (Exception $e) {
                    $this->sugar->handleError($e);
                    $ret = null;
                }

                // store return value
                $stack []= $ret;
                break;
            case 'if':
                $clauses = $code[++$i];
                foreach ($clauses as $clause) {
                    if ($clause[0] === false || $this->execute($vars, $clause[0], $sections)) {
                        $this->execute($vars, $clause[1], $sections);
                        break;
                    }
                }
                break;
            case 'range':
                $step = array_pop($stack);
                $upper = array_pop($stack);
                $lower = array_pop($stack);
                $name = $code[++$i];
                $block = $code[++$i];

                // if step is 0, fail
                if ($step === 0) {
                    throw new Sugar_Exception ('step of 0 in range loop');
                }

                // iterate
                $index = $lower;
                while (($step < 0 && $index >= $upper)
                    || ($step > 0 && $index <= $upper)
                ) {
                    $vars->set($name, $index);
                    $this->execute($vars, $block, $sections);
                    $index += $step;
                }
                break;
            case 'foreach':
                $array = array_pop($stack);
                $key = $code[++$i];
                $name = $code[++$i];
                $block = $code[++$i];
                if (is_array($array) || is_object($array)) {
                    foreach ($array as $k=>$v) {
                        if ($key) {
                            $vars->set($key, $k);
                        }
                        $vars->set($name, $v);
                        $this->execute($vars, $block, $sections);
                    }
                }
                break;
            case 'while':
                $test = $code[++$i];
                $block = $code[++$i];
                while ($this->execute($vars, $test, $sections)) {
                    $this->execute($vars, $block, $sections);
                }
                break;
            case 'nocache':
                $block = $code[++$i];
                if ($this->sugar->cacheHandler) {
                    $this->sugar->cacheHandler->addBlock($block);
                } else {
                    $this->execute($vars, $block, $sections);
                }
                break;
            case '.':
                $index = array_pop($stack);
                $obj = array_pop($stack);
                if (is_array($obj) && isset($obj[$index])) {
                    $stack []= $obj[$index];
                } elseif (is_object($obj) && isset($obj->$index)) {
                    $stack []= $obj->$index;
                } else {
                    $stack []= null;
                }
                break;
            case 'array':
                $elems = $code[++$i];
                $array = array();
                foreach ($elems as $elem) {
                    $array []= $this->execute($vars, $elem, $sections);
                }
                $stack []= $array;
                break;
            default:
                throw new Sugar_Exception(
                    'internal error: unknown opcode `'.$opcode.'`'
                );
            }
        }

        return end($stack);
    }
}

// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
