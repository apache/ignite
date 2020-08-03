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
    private final String token;

    /** Token position. */
    private final int tokenPos;

    /** Token type. */
    private final SqlLexerTokenType tokenTyp;

    /**
     * Constructor.
     *
     * @param sql Original SQL.
     * @param token Token.
     * @param tokenPos Token position.
     * @param tokenTyp Token type.
     */
    public SqlLexerLookAheadToken(String sql, String token, int tokenPos, SqlLexerTokenType tokenTyp) {
        this.sql = sql;
        this.token = token;
        this.tokenPos = tokenPos;
        this.tokenTyp = tokenTyp;
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
}
