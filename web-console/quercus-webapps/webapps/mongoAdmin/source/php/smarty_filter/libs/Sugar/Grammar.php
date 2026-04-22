<?php
/**
 * Template markup grammar parser.
 *
 * Defines the grammar parser engine used for Sugar markup.  This is a hand-
 * written recursive-descent parser.  It's simple in design, but very easy
 * to extend or modify.
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
 * @subpackage Compiler
 * @author     Sean Middleditch <sean@mojodo.com>
 * @author     Shawn Pearce
 * @copyright  2008-2010 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    SVN: $Id: Grammar.php 349 2010-09-16 21:18:47Z Sean.Middleditch $
 * @link       http://php-sugar.net
 * @access     private
 */

/**
 * Source tokenizer.
 */
require_once SUGAR_ROOT.'/Sugar/Lexer.php';

/**
 * Runtime engine, used for optimization.
 */
require_once SUGAR_ROOT.'/Sugar/Runtime.php';

/**
 * Template parser.
 *
 * This class implements the grammar parsing language for the Sugar
 * template language.
 *
 * @category   Template
 * @package    Sugar
 * @subpackage Compiler
 * @author     Sean Middleditch <sean@mojodo.com>
 * @copyright  2008-2009 Mojodo, Inc. and contributors
 * @license    http://opensource.org/licenses/mit-license.php MIT
 * @version    Release: 0.83
 * @link       http://php-sugar.net
 * @access     private
 */
class Sugar_Grammar
{
    /**
     * Tokenizer.
     *
     * @var Sugar_Lexer
     */
    private $_tokens = null;

    /**
     * Stack of bytecode chunks used for expression parsing.
     *
     * @var array
     */
    private $_output = array();

    /**
     * Stack of opcodes used for expression parsing.
     *
     * @var array
     */
    private $_stack = array();

    /**
     * Sugar instance.
     *
     * @var Sugar
     */
    private $_sugar;

    /**
     * Block stack.
     *
     * @var array
     */
    private $_blocks = array();

    /**
     * Sections list.
     *
     * @var array
     */
    private $_sections = array();

    /**
     * Inherited template.
     *
     * @var string
     */
    private $_inherit = null;

    /**
     * Operator precedence map.
     *
     * @var array
     */
    static public $precedence = array(
        '.' => 0, '->' => 0, 'method' => 0, '[' => 0,
        '!' => 1, 'negate' => 1,
        '*' => 2, '/' => 2, '%' => 2,
        '+' => 3, '-' => 3,
        '..' => 4,
        '==' => 5, '<' => 5, '>' => 5,
        '!=' => 5, '<=' => 5, '>=' => 5, 'in' => 5, '!in' => 5,
        '||' => 6, '&&' => 6,
        '(' => 100 // safe wrapper
    );

    /**
     * Constructor.
     *
     * @param Sugar $sugar  Sugar instance.
     */
    public function __construct($sugar)
    {
        $this->_sugar = $sugar;
    }

    /**
     * Parsed out a list of function arguments.
     *
     * @return array Arguments.
     */
    private function _parseFunctionArgs()
    {
        $params = array();
        while (!$this->_tokens->peekAny(array(')', ']', '}', ',', Sugar_Token::TERMINATOR))) {
            // check for name= assignment
            $this->_tokens->expect(Sugar_Token::IDENTIFIER, $name);
            $this->_tokens->expect('=');

            // assign parameter
            $params [$name]= $this->_compileExpr();
        }
        return $params;
    }

    /**
     * Parsed out a list of built-in function arguments.
     *
     * Arguments to built-in functions may, currently, only be strings.
     *
     * The list of argument provided is checked against the list of
     * argumented entered by the user, and an error is thrown if
     * they do not match.
     *
     * @param string $name    Name of the built-in function, for errors.
     * @param array  $require List of required parameters, as name=>type
     *
     * @return array Arguments.
     *
     * @throws Sugar_Parse_Exception
     */
    private function _parseBuiltinFunctionArgs($name, array $require)
    {
        $params = array();

        // keep reading so long as we have identifiers (start of id=value)
        while ($this->_tokens->accept(Sugar_Token::IDENTIFIER, $id)) {
            // check for =value assignment
            $this->_tokens->expect('=');
            $this->_tokens->expect(Sugar_Token::LITERAL, $value);

            // ensure this parameter is allowed
            if (!isset($require[$id])) {
                throw new Sugar_Exception_Parse(
                    $this->_tokens->getFile(),
                    $this->_tokens->getLine(),
                    'unexpected parameter `'.$id.'` to built-in function '.$name
                );
            }

            // check type
            if ($require[$id] != gettype($value)) {
                throw new Sugar_Exception_Parse(
                    $this->_tokens->getFile(),
                    $this->_tokens->getLine(),
                    'wrong type for parameter `'.$id.'` to built-in function '.$name
                );
            }

            // assign parameter
            $params [$id]= $value;
        }

        // ensure all required parameters are set
        foreach ($require as $id=>$type) {
            if (!isset($params[$id])) {
                throw new Sugar_Exception_Parse(
                    $this->_tokens->getFile(),
                    $this->_tokens->getLine(),
                    'missing parameter `'.$id.'` to built-in function '.$name
                );
            }
        }
        
        return $params;
    }

    /**
     * Parsed out a list of method arguments.
     *
     * @return array Arguments.
     */
    private function _parseMethodArgs()
    {
        $params = array();
        while (!$this->_tokens->accept(')')) {
            // assign parameter
            $params []= $this->_compileExpr();

            // if we're at a ), end now
            if ($this->_tokens->accept(')')) {
                break;
            }

            // require a comma after every parameter
            $this->_tokens->expect(',');
        }
        return $params;
    }

    /**
     * Collapses the output and operator stacks for all pending operators
     * under a given precedence level.
     *
     * @param int $level Precedence level to collapse under.
     *
     * @return bool True on success.
     */
    private function _collapseOps($level)
    {
        while ($this->_stack && self::$precedence[end($this->_stack)] <= $level) {
            // get operator
            $op = array_pop($this->_stack);

            // if unary, pop right-hand operand
            if ($op == '!' || $op == 'negate') {
                $right = array_pop($this->_output);
                $this->_output []= array_merge($right, array($op));
            } else {
                // binary, pop both
                $right = array_pop($this->_output);
                $left = array_pop($this->_output);
                $this->_output []= array_merge($left, $right, array($op));
            }
        }
        return true;
    }

    /**
     * Check if a particular bytecode chunk is a push operator (just data)
     * or not.
     *
     * @param array $node Bytecode to check.
     *
     * @return bool True if the node is only data.
     */
    private static function _isData($node)
    {
        return (count($node) == 2 && $node[0] == 'push');
    }

    /**
     * Compile an entire expression.
     *
     * @param bool $modifiers    If set to false, do not look for modifiers
     *                           after the expression.
     * @param bool &$escape_flag Out parameter, set false if escaping should be
     *                           disabled.
     *
     * @return array Bytecode of expression.
     */
    private function _compileExpr($modifiers = true, &$escape_flag = true) {
        // compile a binary expression
        $expr = $this->_compileBinary();

        // look for and apply modifiers, if present
        if ($modifiers && $this->_tokens->accept('|')) {
            return array_merge($expr, $this->_compileModifiers($escape_flag));
        } else {
            return $expr;
        }
    }

    /**
     * Compile a binary expression.
     *
     * @return array Bytecode of expression.
     */
    private function _compileBinary() {
        // wrap operator stack
        $this->_stack []= '(';

        // first left-hand operand (possibly the only node)
        $this->_compileUnary();

        // while we have a binary operator, continue chunking along
        while ($op = $this->_tokens->getOp()) {
            // pop higher precedence operators
            $this->_collapseOps(self::$precedence[$op]);

            // if it's an array or object . or -> op, we can also take a name
            if (($op == '.' || $op == '->') && $this->_tokens->accept(Sugar_Token::IDENTIFIER, $name)) {
                // check if this is a method call
                if ($this->_tokens->accept('(')) {
                    // get name and parameters
                    $method = $name;
                    $params = $this->_parseMethodArgs();

                    // create method call
                    $this->_output []= array_merge(
                        array_pop($this->_output), array(
                            'method', $method, $params,
                            $this->_tokens->getFile(),
                            $this->_tokens->getLine()
                        )
                    );
                } else { // not a method call
                    $this->_stack []= '.';
                    $this->_output []= array('push', $name);
                }

            // if it's an array [] operator, we need to handle the trailing ]
            } elseif ($op == '[') {
                // actual operator is .
                $this->_stack []= '.';

                // compile rest of expression
                $this->_compileUnary();
                $this->_tokens->expect(']');

            // actual opcode for -> is just .
            } else if ($op == '->') {
                $this->_stack []= '.';
                $this->_compileUnary();

            // regular case, just go
            } else {
                $this->_stack []= $op;
                $this->_compileUnary();
            }
        }

        // pop remaining operators
        $this->_collapseOps(10);

        // peel operator stack
        array_pop($this->_stack);

        // remove compiled expression from the output stack
        $expr = array_pop($this->_output);
        return $expr;
    }

    /**
     * Parses a modifier, not include the leading pipe.
     *
     * @param bool &$escape_flag Out parameter, set to false if escaping should
     *                           be disabled.
     *
     * @return array Opcodes
     */
    private function _compileModifiers(&$escape_flag = true)
    {
        $opcodes = array();
        do {
            $this->_tokens->expect(Sugar_Token::IDENTIFIER, $name);

            // parse and compile modifier parameters
            $params = array();
            while ($this->_tokens->accept(':')) {
                $params []= $this->_compileExpr(false);
            }

            // if the modifier was |raw, flag it
            if ($name == 'raw' || $name == 'escape') {
                $escape_flag = false;
            }
            if ($name != 'raw') {
                array_push($opcodes, 'modifier', $name, $params);
            }
        } while ($this->_tokens->accept('|'));

        return $opcodes;
    }

    /**
     * Compiles a single terminal (or unary expression... or a few other
     * constructs.  Not the best named method.  The resulting bytecode
     * is pushed to the output stack.
     *
     * @return bool True on success.
     */
    private function _compileUnary()
    {
        // unary -
        if ($this->_tokens->accept('-')) {
            $this->_stack []= 'negate';
            $this->_compileUnary();

        // unary !
        } elseif ($this->_tokens->accept('!')) {
            $this->_stack []= '!';
            $this->_compileUnary();

        // array constructor
        } elseif ($this->_tokens->accept('[')) {
            // read in elements
            $elems = array();
            $data = true;
            $key = null;
            while (!$this->_tokens->accept(']')) {
                // read in element
                $elem = $this->_compileExpr();

                // if we have a =>, then it must be a key
                // which must be constant
                if ($this->_tokens->accept('=>')) {
                    // array keys must be constant data for now
                    if (!$this->_isData($elem)) {
                        throw new Sugar_Exception_Parse(
                            $this->_tokens->getFile(),
                            $this->_tokens->getLine(),
                            'array keys must be constants'
                        );
                    }
                    $key = $elem[1];

                    // grab actual data
                    $elem = $this->_compileExpr();

                    // put element into array
                    $elems [$key]= $elem;
                } else {
                    // add element to new array
                    $elems []= $elem;
                }

                // if not pure data, unmark data flag
                if ($data && !$this->_isData($elem)) {
                    $data = false;
                }

                // if we have a ], end
                if ($this->_tokens->accept(']')) {
                    break;
                }

                // require a comma before next item
                $this->_tokens->expect(',');
            }

            // if the data flag is true, all elements are pure data,
            // so we can push this as a value instead of an opcode
            if ($data) {
                foreach ($elems as $i=>$v) {
                    $elems[$i] = $v[1];
                }
                $this->_output []= array('push', $elems);
            } else {
                $this->_output []= array('array', $elems);
            }

        // sub-expression
        } elseif ($this->_tokens->accept('(')) {
            // compile sub-expression
            $this->_output []= $this->_compileExpr();

            // ensure trailing )
            $this->_tokens->expect(')');

        // function call
        } elseif ($this->_tokens->accept(Sugar_Token::IDENTIFIER, $name)) {
            // parse modifiers
            $modifiers = array();
            if ($this->_tokens->accept('|')) {
                $modifiers = $this->_compileModifiers();
            }

            // get args
            $params = $this->_parseFunctionArgs();

            // return new function all
            $this->_output []= array_merge(
                array(
                    'call', $name, $params, $this->_tokens->getFile(),
                    $this->_tokens->getLine()
                ),
                $modifiers
            );

        // static values
        } elseif ($this->_tokens->accept(Sugar_Token::LITERAL, $data)) {
            $this->_output []= array('push', $data);

        // vars (last item, expec it)
        } else {
            $this->_tokens->expect(Sugar_Token::VARIABLE, $name);
            $this->_output []= array('lookup', $name);
        }

        return true;
    }

    /**
     * Compile an entire block, or series of statements and raw text.
     *
     * @param string $blockType Type of block being compiled (if, while,
     *                          section, etc.)
     *
     * @return array Block's bytecode.
     */
    public function compileBlock($blockType)
    {
        $block = array();

        // build byte-code
        while (true) {
            // terminators
            if ($this->_tokens->accept(Sugar_Token::EOF)
                || $this->_tokens->acceptKeyword('else')
                || $this->_tokens->acceptKeyword('elseif')
                || $this->_tokens->acceptKeyword('end')
                || $this->_tokens->accept(Sugar_Token::END_BLOCK)
            ) {
                // return token so caller can accept/expect it
                $this->_tokens->unshift();
                break;
            }
            // raw string
            elseif ($this->_tokens->accept(Sugar_Token::DOCUMENT, $literal)) {
                $block []= array('echo', $literal);
            }
            // if the command is empty, ignore
            elseif ($this->_tokens->accept(Sugar_Token::TERMINATOR)) {
                // do nothing
            }
            // flow control - if
            elseif ($this->_tokens->acceptKeyword('if')) {
                // get first clause expr and body
                $ops = $this->_compileExpr();
                $this->_tokens->expect(Sugar_Token::TERMINATOR);
                $body = $this->compileBlock('if');
                $clauses = array(array($ops, $body));

                // get else/else-if clauses
                while (true) {
                    if ($this->_tokens->acceptKeyword('elseif')) {
                        // smarty-style elseif keyword
                        $ops = $this->_compileExpr();
                        $this->_tokens->expect(Sugar_Token::TERMINATOR);
                        $body = $this->compileBlock('else-if');
                        $clauses []= array($ops, $body);
                    } elseif ($this->_tokens->acceptKeyword('else')) {
                        if ($this->_tokens->acceptKeyword('if')) {
                            // handle 'else if' construct
                            $ops = $this->_compileExpr();
                            $this->_tokens->expect(Sugar_Token::TERMINATOR);
                            $body = $this->compileBlock('else-if');
                            $clauses []= array($ops, $body);
                        } else {
                            // plain else
                            $body = $this->compileBlock('else');
                            $clauses []= array(false, $body);

                            // no further else/else-if blocks allowed
                            break;
                        }
                    } else {
                        break;
                    }
                }

                $this->_tokens->expectEndBlock('if');
                $this->_tokens->expect(Sugar_Token::TERMINATOR);

                // push block
                $block []= array('if', $clauses);
            }
            // while loop
            elseif ($this->_tokens->acceptKeyword('while')) {
                // get expression
                $test = $this->_compileExpr();
                $this->_tokens->expect(Sugar_Token::TERMINATOR);

                // get body
                $body = $this->compileBlock('while');
                $this->_tokens->expectEndBlock('while');
                $this->_tokens->expect(Sugar_Token::TERMINATOR);

                // push block
                $block []= array('while', $test, $body);
            }
            // range loop
            elseif ($this->_tokens->acceptKeyword('loop')) {
                // name in lower,upper
                $this->_tokens->expect(Sugar_Token::VARIABLE, $name);
                $this->_tokens->expectKeyword('in');
                $lower = $this->_compileExpr();
                $this->_tokens->expect(',');
                $upper = $this->_compileExpr();

                // optional: ,step
                if ($this->_tokens->accept(',')) {
                    $step = $this->_compileExpr();
                } else {
                    $step = array('push', 1);
                }

                $this->_tokens->expect(Sugar_Token::TERMINATOR);

                // block
                $body = $this->compileBlock('loop');
                $this->_tokens->expectEndBlock('loop');
                $this->_tokens->expect(Sugar_Token::TERMINATOR);

                // push block
                $block []= $lower;
                $block []= $upper;
                $block []= $step;
                $block []= array('range', $name, $body);
            }
            // loop over an array
            elseif ($this->_tokens->acceptKeyword('foreach')) {
                $key = null;
                $name = null;

                // get name
                $this->_tokens->expect(Sugar_Token::VARIABLE, $name);

                // is it a key,name pair?
                if ($this->_tokens->accept(',')) {
                    $key = $name;
                    $this->_tokens->expect(Sugar_Token::VARIABLE, $name);
                }

                // now we need the expression
                $this->_tokens->expectKeyword('in');
                $ops = $this->_compileExpr();
                $this->_tokens->expect(Sugar_Token::TERMINATOR);

                // and the block itself
                $body = $this->compileBlock('foreach');
                $this->_tokens->expectEndBlock('foreach');
                $this->_tokens->expect(Sugar_Token::TERMINATOR);

                // store foreach block
                $block []= $ops;
                $block []= array('foreach', $key, $name, $body);
            }
            // inhibit caching
            elseif ($this->_tokens->acceptKeyword('nocache')) {
                // get block
                $body = $this->compileBlock('nocache');
                $this->_tokens->expectEndBlock('nocache');
                $this->_tokens->expect(Sugar_Token::TERMINATOR);

                $block []= array('nocache', $body);
            }
            // if we have a var, we might have an assignment... or just an expression
            elseif ($this->_tokens->accept(Sugar_Token::VARIABLE, $name)) {
                // if it's followed by a =, it's an assignment
                if ($this->_tokens->accept('=')) {
                    $ops = $this->_compileExpr();
                    $this->_tokens->expect(Sugar_Token::TERMINATOR);

                    $block []= $ops;
                    $block []= array('assign', strtolower($name));
                }
                // otherwise, it's an expression
                else {
                    // put the variable name back, and then compile as an expression
                    $this->_tokens->pushBack();

                    $escape_flag = true;
                    $ops = $this->_compileExpr(true, $escape_flag);
                    $this->_tokens->expect(Sugar_Token::TERMINATOR);

                    $block []= $ops;
                    $block []= array($escape_flag ? 'print' : 'rawprint');
                }
            }
            // new section?
            elseif ($this->_tokens->acceptKeyword('section')) {
                // check if insertion is requested
                $add_insert = false;
                if ($this->_tokens->accept('|')) {
                    $this->_tokens->expectKeyword('insert');
                    $add_insert = true;
                }
 
                // get section identifier
                $params = $this->_parseBuiltinFunctionArgs('section',
                        array('name'=>'string'));
                $this->_tokens->expect(Sugar_Token::TERMINATOR);
                $name = $params['name'];

                // do not allow nested sections
                if ($blockType != 'document') {
                    throw new Sugar_Exception_Parse(
                        $this->_tokens->getFile(),
                        $this->_tokens->getLine(),
                        'sections cannot be defined inside an '.$blockType.' block'
                    );
                }

                // do not allow duplicate sections
                if (isset($this->_sections[$name])) {
                    throw new Sugar_Exception_Parse(
                        $this->_tokens->getFile(),
                        $this->_tokens->getLine(),
                        'section `' . $name . '` already defined'
                    );
                }

                // parse section body
                $body = $this->compileBlock('section');
                $this->_tokens->expectEndBlock('section');

                // store section
                $this->_sections[$name] = $body;

                // add insert instruction if requested
                if ($add_insert) {
                    $block []= array('insert', $name);
                }
            }
            // inherited layout templates
            elseif ($this->_tokens->acceptKeyword('inherit')) {
                // parse arguments; expect a single argument, 'file'
                $params = $this->_parseBuiltinFunctionArgs('inherit',
                        array('file'=>'string'));
                $this->_tokens->expect(Sugar_Token::TERMINATOR);

                // do not allow nested inherited templates
                if ($blockType != 'document') {
                    throw new Sugar_Exception_Parse(
                        $this->_tokens->getFile(),
                        $this->_tokens->getLine(),
                        'inherited template cannot be defined inside an '.$blockType.' block'
                    );
                }

                // do not more than one inherited template
                if (!empty($this->_inherit)) {
                    throw new Sugar_Exception_Parse(
                        $this->_tokens->getFile(),
                        $this->_tokens->getLine(),
                        'inherited template can only be defined once'
                    );
                }

                // store inherited template
                $this->_inherit = $params['file'];
            }
            // insert section
            elseif ($this->_tokens->acceptKeyword('insert')) {
                // parse arguments; expect a single argument, 'name'
                $params = $this->_parseBuiltinFunctionArgs('insert',
                        array('name'=>'string'));
                $this->_tokens->expect(Sugar_Token::TERMINATOR);

                // push opcode
                $block []= array('insert', $params['name']);
            }
            // function call?
            elseif ($this->_tokens->accept(Sugar_Token::IDENTIFIER, $func)) {
                // get modifier, if present
                $modifiers = null;
                $escape_flag = true;
                if ($this->_tokens->accept('|')) {
                    $modifiers = $this->_compileModifiers($escape_flag);
                }

                // parameters
                $params = $this->_parseFunctionArgs();

                // build function call; if we have no modifiers, use 'call_top'
                // optimization; otherwise, use call, pass through modifiers,
                // and then use print/rawprint
                if ($modifiers) {
                    $block []= array('call', $func, $params, $this->_tokens->getFile(),
                        $this->_tokens->getLine());
                    $block []= $modifiers;
                    $block []= array($escape_flag ? 'print' : 'rawprint');
                } else {
                    $block []= array(
                        'call_top', $func, $params, $escape_flag,
                        $this->_tokens->getFile(), $this->_tokens->getLine()
                    );
                }
            }
            // we have a statement
            else {
                $escape_flag = true;
                $ops = $this->_compileExpr(true, $escape_flag);
                $this->_tokens->expect(Sugar_Token::TERMINATOR);

                $block []= $ops;
                $block []= array($escape_flag ? 'print' : 'rawprint');
            }
        }

        // merge the block together
        if (count($block) > 0) {
            $block = call_user_func_array('array_merge', $block);
        }

        return $block;
    }

    /**
     * Compile the given source code into bytecode.
     *
     * @param string $src  Source code to compile.
     * @param string $file Name of the file being compiled.
     *
     * @return array Bytecode.
     */
    public function compile($src, $file = '<input>')
    {
        // create tokenizer
        $this->_tokens = new Sugar_Lexer(
            $src, $file, $this->_sugar->delimStart, $this->_sugar->delimEnd
        );

        // tokenize input
        $this->_tokens->tokenize();

        // build byte-code for content section
        $bytecode = $this->compileBlock('document');
        $this->_tokens->expect(Sugar_Token::EOF);

        // create meta-block
        $code = array(
            'type' => 'ctpl',
            'version' => Sugar::VERSION,
            'bytecode' => $bytecode,
            'inherit' => $this->_inherit,
            'sections' => $this->_sections,
        );

        // free tokenizer
        $this->_tokens = null;

        // free sections array
        $this->_sections = array();

        return $code;
    }
}
// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
