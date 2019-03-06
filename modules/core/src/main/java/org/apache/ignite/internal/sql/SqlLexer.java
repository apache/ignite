/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.sql;

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
                            throw new SqlParseException(sql, tokenStartPos0, IgniteQueryErrorCode.PARSING,
                                "Unclosed quoted identifier.");
                        }

                        char c1 = inputChars[pos];

                        pos++;

                        if (c1 == '\"')
                            break;
                    }

                    token0 = sql.substring(tokenStartPos0 + 1, pos - 1);
                    tokenTyp0 = SqlLexerTokenType.QUOTED;

                    break;

                case '\'':
                    while (true) {
                        if (eod()) {
                            throw new SqlParseException(sql, tokenStartPos0, IgniteQueryErrorCode.PARSING,
                                "Unclosed string constant.");
                        }

                        char c1 = inputChars[pos];

                        pos++;

                        if (c1 == '\'') {
                            char c2 = inputChars[pos];

                            if (c2 == '\'')
                                pos++;
                            else
                                break;
                        }
                    }

                    token0 = sql.substring(tokenStartPos0 + 1, pos - 1).replaceAll("''", "'");
                    tokenTyp0 = SqlLexerTokenType.STRING;

                    break;

                case '.':
                case ',':
                case ';':
                case '(':
                case ')':
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
                    tokenTyp0 = SqlLexerTokenType.DEFAULT;
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
    @Override public String sql() {
        return sql;
    }

    /** {@inheritDoc} */
    @Override public String token() {
        return token;
    }

    /** {@inheritDoc} */
    @Override public char tokenFirstChar() {
        assert tokenTyp != SqlLexerTokenType.EOF;

        return token.charAt(0);
    }

    /** {@inheritDoc} */
    @Override public int tokenPosition() {
        return tokenPos;
    }

    /** {@inheritDoc} */
    @Override public SqlLexerTokenType tokenType() {
        return tokenTyp;
    }

    /**
     * @return {@code True} if end of data is reached.
     */
    public boolean eod() {
        return pos == inputChars.length - 1;
    }

    /**
     * @return Current lexer position.
     */
    public int position() {
        return pos;
    }
}
