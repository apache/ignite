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

/**
 * SQL lexer.
 */
public class SqlLexer {
    /** Original input. */
    private final String input;

    /** Input characters. */
    private final char[] inputChars;

    /** Current position. */
    private int pos;

    /** Current token start. */
    private int tokenStartPos;

    /** Current token. */
    private String token;

    /** Token type. */
    private SqlLexerTokenType tokenTyp;

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
     * Shift lexer to the next position.
     *
     * @return {@code True} if more tokens are available.
     */
    public boolean shift() {
        while (!eod()) {
            String token0 = null;
            int tokenStartPos0 = pos;
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
                        if (eod())
                            throw new RuntimeException();

                        char c1 = inputChars[pos];

                        pos++;

                        if (c1 == '\"')
                            break;
                    }

                    token0 = input.substring(tokenStartPos0 + 1, pos - 1);

                    tokenTyp0 = SqlLexerTokenType.QUOTED;

                    break;

                case '.':
                    token0 = ".";

                    tokenTyp0 = SqlLexerTokenType.DOT;

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

            if (token0 != null) {
                token = token0;
                tokenStartPos = tokenStartPos0;
                tokenTyp = tokenTyp0;

                return true;
            }
        }

        return false;
    }

    /**
     * Shift lexer and get the next token or {@code null} if none available.
     *
     * @return Next token or {@code null}.
     */
    public String shiftAndGet() {
        if (shift())
            return token;
        else
            return null;
    }

    /**
     * @return {@code True} if end of data is reached.
     */
    private boolean eod() {
        return pos == inputChars.length - 1;
    }

    /**
     * @return Current token.
     */
    public String token() {
        return token;
    }

    /**
     * @return Current token start position.
     */
    public int tokenStartPosition() {
        return tokenStartPos;
    }

    /**
     * @return Token type.
     */
    public SqlLexerTokenType tokenType() {
        return tokenTyp;
    }
}
