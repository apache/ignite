/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql;

import java.util.HashMap;

/**
 * SQL lexer.
 */
public class SqlLexer implements SqlParserToken {
    /** Simple token types. */
    private static final HashMap<Character, SqlLexerTokenType> SIMPLE_TOKEN_TYPS = new HashMap<>();

    /** Original input. */
    private final String input;

    /** Input characters. */
    private final char[] inputChars;

    /** Current position. */
    private int pos;

    /** Current token start. */
    private int tokenPos;

    /** Current token. */
    private String token;

    /** Token type. */
    private SqlLexerTokenType tokenTyp;

    static {
        SIMPLE_TOKEN_TYPS.put('.', SqlLexerTokenType.DOT);
        SIMPLE_TOKEN_TYPS.put(',', SqlLexerTokenType.COMMA);
        SIMPLE_TOKEN_TYPS.put(';', SqlLexerTokenType.SEMICOLON);
        SIMPLE_TOKEN_TYPS.put('(', SqlLexerTokenType.PARENTHESIS_LEFT);
        SIMPLE_TOKEN_TYPS.put(')', SqlLexerTokenType.PARENTHESIS_RIGHT);
    }

    /**
     * Constructor.
     *
     * @param input Input.
     */
    public SqlLexer(String input) {
        assert input != null;

        this.input = input;

        // Additional slot for look-ahead convenience.
        inputChars = new char[input.length() + 1];

        for (int i = 0; i < input.length(); i++)
            inputChars[i] = input.charAt(i);
    }

    /**
     * Copying constructor.
     *
     * @param other Other instance.
     */
    private SqlLexer(SqlLexer other) {
        this.input = other.input;
        this.inputChars = other.inputChars;
        this.pos = other.pos;
        this.tokenPos = other.tokenPos;
        this.token = other.token;
        this.tokenTyp = other.tokenTyp;
    }

    /**
     * Get next token without lexer state change.
     *
     * @return Next token or {@code null} of end is reached.
     */
    public SqlParserToken lookAhead() {
        int pos0  = pos;
        String token0 = token;
        int tokenPos0 = tokenPos;
        SqlLexerTokenType tokenTyp0 = tokenTyp;

        try {
            if (shift())
                return new SqlParserTokenImpl(token, tokenPos, tokenTyp);

            return null;
        }
        finally {
            pos = pos0;
            token = token0;
            tokenPos = tokenPos0;
            tokenTyp = tokenTyp0;
        }
    }

    /**
     * Shift lexer to the next position.
     *
     * @return {@code True} if next token was found, {@code false} in case of end-of-file.
     */
    public boolean shift() {
        while (!eod()) {
            int tokenStartPos0 = pos;

            String token0 = null;
            SqlLexerTokenType tokenTyp0 = null;

            char c = inputChars[pos++];

            switch (c) {
                case '-':
                    if (inputChars[pos] == '-') {
                        // Full-line comment.
                        pos++;

                        while (!eod()) {
                            char c1 = inputChars[pos];

                            if (c1 == '\n' || c1 == '\r')
                                break;

                            pos++;
                        }
                    }
                    else {
                        // Minus.
                        token0 = "-";
                        tokenTyp0 = SqlLexerTokenType.MINUS;
                    }

                    break;

                case '\"':
                    while (true) {
                        if (eod()) {
                            throw new SqlParseException(input, tokenStartPos0, "Unclosed quoted identifier.");
                        }

                        char c1 = inputChars[pos];

                        pos++;

                        if (c1 == '\"')
                            break;
                    }

                    token0 = input.substring(tokenStartPos0 + 1, pos - 1);
                    tokenTyp0 = SqlLexerTokenType.QUOTED;

                    break;

                case '.':
                case ',':
                case ';':
                case '(':
                case ')':
                    token0 = Character.toString(c);
                    tokenTyp0 = SIMPLE_TOKEN_TYPS.get(c);

                    break;

                default:
                    if (c <= ' ' || Character.isSpaceChar(c))
                        continue;

                    while (!eod()) {
                        char c1 = inputChars[pos];

                        if (!Character.isJavaIdentifierPart(c1))
                            break;

                        pos++;
                    }

                    token0 = input.substring(tokenStartPos0, pos).toUpperCase();
                    tokenTyp0 = SqlLexerTokenType.DEFAULT;
            }

            if (tokenTyp0 != null) {
                token = token0;
                tokenPos = tokenStartPos0;
                tokenTyp = tokenTyp0;

                return true;
            }
        }

        return false;
    }

    /**
     * Fork lexer.
     *
     * @return New lexer.
     */
    public SqlLexer fork() {
        return new SqlLexer(this);
    }

    /** {@inheritDoc} */
    public String token() {
        return token;
    }

    /** {@inheritDoc} */
    public char tokenFirstChar() {
        return token.charAt(0);
    }

    /** {@inheritDoc} */
    public int tokenPosition() {
        return tokenPos;
    }

    /** {@inheritDoc} */
    public SqlLexerTokenType tokenType() {
        return tokenTyp;
    }

    /**
     * @return Input.
     */
    public String input() {
        return input;
    }

    /**
     * @return {@code True} if end of data is reached.
     */
    private boolean eod() {
        return pos == inputChars.length - 1;
    }
}
