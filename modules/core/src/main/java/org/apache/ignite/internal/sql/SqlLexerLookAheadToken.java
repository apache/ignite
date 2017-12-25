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
 * Plain immutable look-ahead parser token.
 */
public class SqlLexerLookAheadToken implements SqlLexerToken {
    /** SQL. */
    private final String sql;

    /** Token. */
    private final String tok;

    /** Token position. */
    private final int tokPos;

    /** Token type. */
    private final SqlLexerTokenType tokTyp;

    /**
     * Constructor.
     *
     * @param sql Original SQL.
     * @param tok Token.
     * @param tokPos Token position.
     * @param tokTyp Token type.
     */
    public SqlLexerLookAheadToken(String sql, String tok, int tokPos, SqlLexerTokenType tokTyp) {
        this.sql = sql;
        this.tok = tok;
        this.tokPos = tokPos;
        this.tokTyp = tokTyp;
    }

    /** {@inheritDoc} */
    public String sql() {
        return sql;
    }

    /** {@inheritDoc} */
    @Override public String token() {
        return tok;
    }

    /** {@inheritDoc} */
    @Override public char tokenFirstChar() {
        return tok.charAt(0);
    }

    /** {@inheritDoc} */
    @Override public int tokenPosition() {
        return tokPos;
    }

    /** {@inheritDoc} */
    @Override public SqlLexerTokenType tokenType() {
        return tokTyp;
    }
}
