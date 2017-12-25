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

    /** Current tok start. */
    private int tokPos;

    /** Current tok. */
    private String tok;

    /** Token type. */
    private SqlLexerTokenType tokTyp;

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
     * Get next tok without lexer state change.
     *
     * @return Next tok.
     */
    public SqlLexerToken lookAhead() {
        int pos0  = pos;
        String tok0 = tok;
        int tokPos0 = tokPos;
        SqlLexerTokenType tokTyp0 = tokTyp;

        try {
            if (shift())
                return new SqlLexerLookAheadToken(sql, tok, tokPos, tokTyp);
            else
                return new SqlLexerLookAheadToken(sql, null, tokPos, SqlLexerTokenType.EOF);
        }
        finally {
            pos = pos0;
            tok = tok0;
            tokPos = tokPos0;
            tokTyp = tokTyp0;
        }
    }

    /**
     * Gets the current tok as SqlLexerToken object.
     *
     * @return The current tok object.
     */
    public SqlLexerToken currentToken() {
        return new SqlLexerLookAheadToken(sql, tok, tokPos, tokTyp);
    }

    /**
     * Shift lexer to the next position.
     *
     * @return {@code True} if next tok was found, {@code false} in case of end-of-file.
     */
    public boolean shift() {
        while (!eod()) {
            int tokStartPos0 = pos;

            String tok0 = null;
            SqlLexerTokenType tokTyp0 = null;

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
                        tok0 = "-";
                        tokTyp0 = SqlLexerTokenType.MINUS;
                    }

                    break;

                case '"':
                case '\'': {
                    StringBuilder sb = new StringBuilder(inputChars.length - pos);

                    while (true) {
                        if (eod()) {
                            throw new SqlParseException(sql, tokStartPos0, IgniteQueryErrorCode.PARSING,
                                "Unclosed quoted identifier.");
                        }

                        char c1 = inputChars[pos];
                        pos++;

                        // Escaped character
                        if (convertBackslashEscapes && c1 == '\\' && c == '"') {

                            SqlEscSeqParser escParser = new SqlEscSeqParser();

                            while (true) {
                                if (eod()) {
                                    throw new SqlParseException(sql, tokStartPos0, IgniteQueryErrorCode.PARSING,
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
                                    throw new SqlParseException(sql, tokStartPos0, IgniteQueryErrorCode.PARSING,
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

                    tok0 = sb.toString();
                    tokTyp0 = (c == '"') ? SqlLexerTokenType.DBL_QUOTED : SqlLexerTokenType.SGL_QUOTED;

                    break;
                }

                case '.':
                case ',':
                case ';':
                case '(':
                case ')':
                case '=':
                    tok0 = Character.toString(c);
                    tokTyp0 = SqlLexerTokenType.forChar(c);

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

                    tok0 = sql.substring(tokStartPos0, pos).toUpperCase();
                    tokTyp0 = SqlLexerTokenType.KEYWORD;
            }

            if (tokTyp0 != null) {
                tok = tok0;
                tokPos = tokStartPos0;
                tokTyp = tokTyp0;

                return true;
            }
        }

        tok = null;
        tokPos = pos;
        tokTyp = SqlLexerTokenType.EOF;

        return false;
    }

    /** {@inheritDoc} */
    public String sql() {
        return sql;
    }

    /** {@inheritDoc} */
    public String token() {
        return tok;
    }

    /** {@inheritDoc} */
    public char tokenFirstChar() {
        assert tokTyp != SqlLexerTokenType.EOF;

        return tok.charAt(0);
    }

    /** {@inheritDoc} */
    public int tokenPosition() {
        return tokPos;
    }

    /** {@inheritDoc} */
    public SqlLexerTokenType tokenType() {
        return tokTyp;
    }

    /**
     * @return {@code True} if end of data is reached.
     */
    private boolean eod() {
        return pos == inputChars.length - 1;
    }
}
