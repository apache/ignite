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

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;

/**
 * SQL lexer.
 */
public class SqlLexer implements SqlLexerToken {
    /** Original input. */
    private final String sql;

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

    /** Detect and convert backslash escape sequences */
    private final boolean convertBackslashEscapes;

    /**
     * Constructor.
     *
     * @param sql Input.
     */
    public SqlLexer(String sql) {
        assert sql != null;

        this.sql = sql;

        // Additional slot for look-ahead convenience.
        inputChars = new char[sql.length() + 1];

        for (int i = 0; i < sql.length(); i++)
            inputChars[i] = sql.charAt(i);

        convertBackslashEscapes = !IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SQL_PARSER_BACKSLASH_ESCAPES_DISABLED);
    }

    /**
     * Get next token without lexer state change.
     *
     * @return Next token.
     */
    public SqlLexerToken lookAhead() {
        int pos0  = pos;
        String token0 = token;
        int tokenPos0 = tokenPos;
        SqlLexerTokenType tokenTyp0 = tokenTyp;

        try {
            if (shift())
                return new SqlLexerLookAheadToken(sql, token, tokenPos, tokenTyp);
            else
                return new SqlLexerLookAheadToken(sql, null, tokenPos, SqlLexerTokenType.EOF);
        }
        finally {
            pos = pos0;
            token = token0;
            tokenPos = tokenPos0;
            tokenTyp = tokenTyp0;
        }
    }

    /**
     * Gets the current token as SqlLexerToken object.
     *
     * @return The current token object.
     */
    public SqlLexerToken currentToken() {
        return new SqlLexerLookAheadToken(sql, token, tokenPos, tokenTyp);
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

                case '"':
                case '\'': {
                    StringBuilder sb = new StringBuilder(inputChars.length - pos);

                    while (true) {
                        if (eod()) {
                            throw new SqlParseException(sql, tokenStartPos0, IgniteQueryErrorCode.PARSING,
                                "Unclosed quoted identifier.");
                        }

                        char c1 = inputChars[pos];
                        pos++;

                        // Escaped character
                        if (convertBackslashEscapes && c1 == '\\' && c == '"') {

                            SqlEscSeqParser escParser = new SqlEscSeqParser();

                            while (true) {
                                if (eod()) {
                                    throw new SqlParseException(sql, tokenStartPos0, IgniteQueryErrorCode.PARSING,
                                        "Nonterminated escape sequence in quoted identifier.");
                                }

                                c1 = inputChars[pos];

                                SqlEscSeqParser.State state = escParser.accept(c1);

                                if (state == SqlEscSeqParser.State.FINISHED_CHAR_ACCEPTED) {
                                    pos++;
                                    break;
                                }

                                if (state == SqlEscSeqParser.State.FINISHED_CHAR_REJECTED)
                                    break;

                                if (state == SqlEscSeqParser.State.ERROR)
                                    throw new SqlParseException(sql, tokenStartPos0, IgniteQueryErrorCode.PARSING,
                                        "Character cannot be part of escape sequence: '" + c1 + "'");

                                assert state == SqlEscSeqParser.State.PROCESSING;

                                pos++;
                            }

                            sb.append(escParser.convertedStr());
                            continue;
                        }

                        if (c1 == c) {
                            if (!eod() && inputChars[pos] == c) { // Process doubled quotes
                                sb.append(c1);

                                pos++;

                                continue;
                            }
                            else
                                break; // Terminate on ending quote
                        }

                        sb.append(c1);
                    }

                    token0 = sb.toString();
                    tokenTyp0 = (c == '"') ? SqlLexerTokenType.DBL_QUOTED : SqlLexerTokenType.SGL_QUOTED;

                    break;
                }

                case '.':
                case ',':
                case ';':
                case '(':
                case ')':
                case '=':
                    token0 = Character.toString(c);
                    tokenTyp0 = SqlLexerTokenType.forChar(c);

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

                    token0 = sql.substring(tokenStartPos0, pos).toUpperCase();
                    tokenTyp0 = SqlLexerTokenType.KEYWORD;
            }

            if (tokenTyp0 != null) {
                token = token0;
                tokenPos = tokenStartPos0;
                tokenTyp = tokenTyp0;

                return true;
            }
        }

        token = null;
        tokenPos = pos;
        tokenTyp = SqlLexerTokenType.EOF;

        return false;
    }

    /** {@inheritDoc} */
    public String sql() {
        return sql;
    }

    /** {@inheritDoc} */
    public String token() {
        return token;
    }

    /** {@inheritDoc} */
    public char tokenFirstChar() {
        assert tokenTyp != SqlLexerTokenType.EOF;

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
     * @return {@code True} if end of data is reached.
     */
    private boolean eod() {
        return pos == inputChars.length - 1;
    }
}
