<?php
/**
 * Sugar template language tokenizer.
 *
 * The tokenizer is responsible for taking template files and breaking them
 * into a series of tokens to be consumed by the grammar parser portion of
 * the Sugar compiler.  The tokenizer makes use of some rather ugly
 * regular expressions as they actually provide better performance than any
 * other method available in PHP for this purpose.
 *
 * The regular expressions could possible use some improvements in efficiency.
 * In particular, even though the compiled regex is cached by PHP, the string
 * interpolation done on each token loop to build the regex should be avoied.
 * This is the most used and most performance-sensitive portion of the
 * compiler, and as such needs more love than the grammar when it comes time
 * to optimize.
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
 * @version    SVN: $Id: Lexer.php 347 2010-09-15 17:11:23Z Sean.Middleditch $
 * @link       http://php-sugar.net
 * @access     private
 */

/**
 * Source token class.
 */
require_once SUGAR_ROOT.'/Sugar/Token.php';

/**
 * Sugar language tokenizer.
 *
 * Tokenizes a source file for use by {@link Sugar_Grammar}.
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
class Sugar_Lexer
{
    /**
     * Source code to be tokenized.
     *
     * @var string
     */
    private $_src;

    /**
     * List of tokens for the input source
     *
     * @var array
     */
    private $_tokens = array();

    /**
     * Next token index for the accept/expect methods.
     *
     * @var int
     */
    private $_next = 0;

    /**
     * The name of the source file being tokenized.
     *
     * @var string
     */
    private $_file;

    /**
     * Starting delimiter.
     *
     * @var string
     */
    private $_delimStart = '{{';

    /**
     * Ending delimiter.
     *
     * @var string
     */
    private $_delimEnd = '}}';

    /**
     * Constructor.
     *
     * @param string $src        The source code to tokenizer.
     * @param string $file       The name of the file being tokenized.
     * @param string $delimStart Start delimiter for template code.
     * @param string $delimEnd   End delimiter for template code.
     */
    public function __construct($src, $file, $delimStart, $delimEnd)
    {
        $this->_src = $src;
        $this->_file = $file;
        $this->_delimStart = $delimStart;
        $this->_delimEnd = $delimEnd;
    }

    /**
     * Converts backslash escape sequences in strings to the proper value.
     * Only supports double-backslash and backslash-n (newline) currently.
     *
     * @param string $string String to decode.
     *
     * @return string Decoded string.
     */
    public static function decodeSlashes($string)
    {
        $string = str_replace('\\n', "\n", $string);
        $string = stripslashes($string);
        return $string;
    }

    /**
     * Parses the entire source into a list of tokens.
     *
     * @throws Sugar_Exception_Parse on invalid template input.
     */
    public function tokenize()
    {
        $inCmd  = false;       // true if we have encountered an opening delimitero
        $source = $this->_src; // the source code being lexed
        $pos    = 0;           // position in source code
        $line   = 1;           // line index into source code

        // loop until we hit the end of the string
        $srclen = strlen($source);
        $i = 0;
        while ($pos < $srclen) {
            // outside of a command?
            if (!$inCmd) {
                // find next opening delimiter
                $next = strpos($source, $this->_delimStart, $pos);

                // set $next to last byte
                if ($next === false) {
                    $next = strlen($source);
                }

                // just a literal?
                if ($next > $pos) {
                    // add text token
                    $text = substr($source, $pos, $next - $pos);
                    $this->_tokens[] = new Sugar_Token(Sugar_Token::DOCUMENT, $line, $text);

                    // update position and line
                    $line += substr_count(
                        $source,
                        "\n",
                        $pos,
                        $next - $pos
                    );
                    $pos = $next;

                    // return to top of main loop
                    continue;
                }

                // setup inside command
                $inCmd = true;
                $pos = $next + 2;
            }

            // skip spaces and comments
            while (preg_match('/(?:\s+|(?:\/\*.*?\*\/|\/\/.*?($|'.preg_quote($this->_delimEnd).')))/msA', $source, $ar, 0, $pos)) {
                // increment position and line
                $pos += strlen($ar[0]);
                $line += substr_count($ar[0], "\n");

                // line comment ended with an end delimiter
                if (isset($ar[1]) && $ar[1] === $this->_delimEnd) {
                    $inCmd = false;
                    break;
                }
            }

            // line comment ended with an end delimiter, part deux;
            // add token and go back to top of main loop
            if (!$inCmd) {
                $this->_tokens[] = new Sugar_Token(Sugar_Token::TERMINATOR, $line, $this->_delimEnd);
                continue;
            }

            // get next token
            if (!preg_match('/(?:'.preg_quote($this->_delimEnd).'|\$(\w+)|(\d+(?:[.]\d+)?)|\/([A-Za-z_]\w+)|(\w+)|==|!=|!in\b|<=|>=|=>|\|\||&&|->|[.][.]|.)/msA', $source, $token, 0, $pos)) {
                throw new Sugar_Exception_Parse(
                    $this->_file,
                    $line,
                    'garbage at: '.substr($source, $pos, 12)
                );
            }

            // increment position by size of token
            $pos += strlen($token[0]);

            // double-quoted string
            if ($token[0] === '"') {
                if (!preg_match('/((?:[^"\\\\]*\\\\.)*[^"]*)"/msA', $source, $string, 0, $pos)) {
                    throw new Sugar_Exception_Parse(
                        $this->_file,
                        $line,
                        'unterminated string constant at: '.
                            substr($source, $pos, 12)
                    );
                }

                // add token
                $this->_tokens[] = new Sugar_Token(Sugar_Token::LITERAL, $line, self::decodeSlashes($string[1]));

                // increment position and line
                $pos += strlen($string[0]);
                $line += substr_count($string[0], "\n");
            }
            // single-quoted string
            elseif ($token[0] === '\'') {
                if (!preg_match('/((?:[^\'\\\\]*\\\\.)*[^\']*)\'/msA', $source, $string, 0, $pos)) {
                    throw new Sugar_Exception_Parse(
                        $this->_file,
                        $line,
                        'unterminated string constant at: '.
                            substr($source, $pos, 12)
                    );
                }

                // add token
                $this->_tokens[] = new Sugar_Token(Sugar_Token::LITERAL, $line, self::decodeSlashes($string[1]));

                // increment position and line
                $pos += strlen($string[0]);
                $line += substr_count($string[0], "\n");
            }
            // variable
            elseif (isset($token[1]) && $token[1] !== '') {
                $this->_tokens[] = new Sugar_Token(Sugar_Token::VARIABLE, $line, $token[1]);
            }
            // end delimiter
            // if at end, mark that
            elseif ($token[0] === $this->_delimEnd) {
                $inCmd = false;
                $this->_tokens[] = new Sugar_Token(Sugar_Token::TERMINATOR, $line, $token[0]);
            }
            // statement terminator
            elseif ($token[0] === ';') {
                $this->_tokens[] = new Sugar_Token(Sugar_Token::TERMINATOR, $line, $token[0]);
            }
            // block terminator
            elseif (isset($token[3]) && $token[3] !== '') {
                $this->_tokens[] = new Sugar_Token(Sugar_Token::END_BLOCK, $line, $token[3]);
            }
            // floating point number
            elseif (isset($token[2]) && $token[2] !== ''
                && strpos($token[2], '.') !== false
            ) {
                $this->_tokens[] = new Sugar_Token(Sugar_Token::LITERAL, $line, floatval($token[2]));
            }
            // integer
            elseif (isset($token[2]) && $token[2] !== '') {
                $this->_tokens[] = new Sugar_Token(Sugar_Token::LITERAL, $line, intval($token[2]));
            }
            // and and or
            elseif ($token[0] === 'and') {
                $this->_tokens[] = new Sugar_Token('&&', $line, $token[0]);
            } elseif ($token[0] === 'or') {
                $this->_tokens[] = new Sugar_Token('||', $line, $token[0]);
            }
            // true and false
            elseif ($token[0] === 'true') {
                $this->_tokens[] = new Sugar_Token(Sugar_Token::LITERAL, $line, true);
            } elseif ($token[0] === 'false') {
                $this->_tokens[] = new Sugar_Token(Sugar_Token::LITERAL, $line, false);
            }
            // identifier
            elseif (isset($token[4]) && $token[4] !== '') {
                $this->_tokens[] = new Sugar_Token(Sugar_Token::IDENTIFIER, $line, $token[4]);
            }
            // generic operator
            else {
                $this->_tokens[] = new Sugar_Token($token[0], $line, $token[0]);
            }
        }

        // append EOF token
        $this->_tokens[] = new Sugar_Token(Sugar_Token::EOF, $line, null);
    }

    /**
     * Pushes a previously accepted/expected token back into the front of
     * the token queue
     */
    public function pushBack() {
        --$this->_next;
    }

    /**
     * Checks to the see if the next token matches the requested token
     * type.  If it does, the token is consumed.  The token data is
     * stored in the second parameter.  True is returned if the token
     * was consumed, and false otherwise.
     *
     * @param string $accept Which token type to accept.
     * @param mixed  &$data  Token token.
     *
     * @return bool True if the token matched.
     */
    public function accept($accept, &$data = null)
    {
        $token = $this->_tokens[$this->_next];

        // return false if it's the wrong token
        if ($token->type != $accept) {
            return false;
        }

        // store data
        $data = $token->extra;

        // increment for next call
        ++$this->_next;
        return true;
    }

    /**
     * Checks to the see if the next token matches the requested keyword.
     * Unlike most languages, keywords in Sugar can be used as regular
     * identifiers, so long as their placement does not conflict with the
     * grammar.
     *
     * @param mixed $accept Which identifier to expect.
     *
     * @return bool True on success.
     */
    public function acceptKeyword($accept)
    {
        $token = $this->_tokens[$this->_next];

        // return false if it's the wrong token
        if ($token->type != Sugar_Token::IDENTIFIER || $token->extra != $accept) {
            return false;
        }

        // increment for next call
        ++$this->_next;
        return true;
    }

    /**
     * Checks to see if the next token is one of a list of given types.
     *
     * @param array $accept Tokens to accept.
     *
     * @return bool True if one of the given token types matches.
     */
    public function peekAny(array $accept)
    {
        $token = $this->_tokens[$this->_next];
        return in_array($token->type, $accept);
    }
    
    /**
     * Throws an expected-token exception
     *
     * @param string $expected The type of token expected
     *
     * @throws Sugar_Exception_Parse always
     */
    private function throwExpect($expected)
    {
        $token = $this->_tokens[$this->_next];

        // build message
        $msg = 'expected '.Sugar_Token::getTypeName($expected);
        if ($this->_next != 0) {
            $msg .= ' after '.$this->_tokens[$this->_next - 1]->getPrettyName();
        }
        $msg .= '; found '.$token->getPrettyName();

        throw new Sugar_Exception_Parse($this->_file, $token->line, $msg);
    }

    /**
     * Checks to the see if the next token matches the requested token
     * type.  If it does, the token is consumed.  The token data is
     * stored in the second parameter.  If the token does not match,
     * a {@link Sugar_Exception_Parse} is raised.
     *
     * @param mixed $expect Which token type to accept, or a list of tokens.
     * @param mixed &$data  Token token.
     *
     * @return bool True on success.
     * @throws Sugar_Exception_Parse when the next token does not match $expect.
     */
    public function expect($expect, &$data = null)
    {
        $token = $this->_tokens[$this->_next];

        // throw an error if it's the wrong token
        if (is_array($expect)) {
            if (!in_array($token->type, $expect)) {
                $this->throwExpect(implode(' or ', $expect));
            }
        } else {
            if ($token->type != $expect) {
                $this->throwExpect($expect);
            }
        }

        // store value
        $data = $token->extra;

        // increment for next call
        ++$this->_next;
        return true;
    }

    /**
     * Checks to the see if the next token matches the requested keyword.
     * Unlike most languages, keywords in Sugar can be used as regular
     * identifiers, so long as their placement does not conflict with the
     * grammar.
     *
     * @param mixed $expect Which identifier to expect.
     *
     * @return bool True on success.
     * @throws Sugar_Exception_Parse when the next token does not match $expect.
     */
    public function expectKeyword($expect)
    {
        $token = $this->_tokens[$this->_next];

        // every keyword is an identifier
        if ($token->type != Sugar_Token::IDENTIFIER || $token->extra != $expect) {
            $this->throwExpect($expect);
        }

        // increment for next call
        ++$this->_next;
        return true;
    }

    /**
     * Block terminator expect wrapper.
     *
     * @param string $name Block name to expect.
     *
     * @return bool True on success.
     * @throws Sugar_Exception_Parse
     */
    public function expectEndBlock($name)
    {
        $token = $this->_tokens[$this->_next];

        if ($token->type != 'end' // back-compat
            && ($token->type != Sugar_Token::END_BLOCK || $token->extra != $name)
        ) {
            $this->throwExpect('/'.$name);
        }

        // increment for next call
        ++$this->_next;
        return true;
    }
    
    /**
     * Pull the next token from the front of the queue and increment
     * the next-token pointer.
     *
     * @return Sugar_Token
     */
    public function shift()
    {
        return $this->_tokens[$this->_next++];
    }

    /**
     * Push the last accepted/expected token back into the queue.
     */
    public function unshift()
    {
        --$this->_next;
    }

    /**
     * Similar to {@link Sugar_Lexer::expect}, except that it checks for
     * any of standard Sugar operators, and the matched operator (if any)
     * is returned.
     *
     * @return mixed The operator if one matches, or false otherwise.
     */
    public function getOp()
    {
        $token = $this->_tokens[$this->_next];
        $op = $token->type;

        // convert = to == for operators
        if ($op == '=') {
            $op = '==';
        }

        // identifiers can be operators too
        if ($op == Sugar_Token::IDENTIFIER) {
            $op = $token->extra;
        }

        // if it's a valid operator, return it
        if (isset(Sugar_Grammar::$precedence[$op])) {
            // increment for next call
            ++$this->_next;

            return $op;
        } else {
            return false;
        }
    }

    /**
     * Returns the current line the tokenizer is at.
     *
     * @return int Line number.
     */
    public function getLine()
    {
        return $this->_tokens[$this->_next]->line;
    }

    /**
     * Returns the file name given to the constructor.
     *
     * @return string File name.
     */
    public function getFile()
    {
        return $this->_file;
    }
}
// vim: set expandtab shiftwidth=4 tabstop=4 :
?>
