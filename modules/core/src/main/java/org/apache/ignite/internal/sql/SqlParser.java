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

import org.apache.ignite.internal.sql.command.SqlCommand;

import static org.apache.ignite.internal.sql.SqlKeyword.CREATE;
import static org.apache.ignite.internal.sql.SqlKeyword.DROP;

/**
 * SQL parser.
 */
public class SqlParser {
    /** Lexer. */
    private final SqlLexer lex;

    /**
     * Constructor.
     *
     * @param sql Original SQL.
     */
    public SqlParser(String sql) {
        lex = new SqlLexer(sql);
    }

    /**
     * Get next command.
     *
     * @return Command or {@code null} if end of script is reached.
     */
    public SqlCommand nextCommand() {
        while (true) {
            if (!lex.shift())
                return null;

            switch (lex.tokenType()) {
                case SEMICOLON:
                    // Empty command, skip.
                    continue;

                case DEFAULT:
                    switch (lex.tokenFirstChar()) {
                        case 'C':
                            if (matches(CREATE))
                                return processCreate();

                            break;

                        case 'D':
                            if (matches(DROP))
                                return processDrop();

                            break;

                        default:
                            throw exceptionUnexpectedToken();
                    }

                    throw exceptionUnexpectedToken();

                case QUOTED:
                case MINUS:
                case DOT:
                case COMMA:
                case PARENTHESIS_LEFT:
                case PARENTHESIS_RIGHT:
                default:
                    throw exceptionUnexpectedToken();
            }
        }
    }

    /**
     * Process CREATE keyword.
     *
     * @return Command.
     */
    private SqlCommand processCreate() {
        // TODO

        return null;
    }

    /**
     * Process DROP keyword.
     *
     * @return Command.
     */
    private SqlCommand processDrop() {
        // TODO

        return null;
    }

    /**
     * Check if current lexer token matches expected.
     *
     * @param expToken Expected token.
     * @return {@code True} if matches.
     */
    private boolean matches(String expToken) {
        String token = lex.token();

        return expToken.equals(token);
    }

    /**
     * @return Original SQL.
     */
    public String sql() {
        return lex.input();
    }

    /**
     * Create parse exception referring to current lexer position.
     *
     * @param msg Message.
     * @return Exception.
     */
    private SqlParseException exception(String msg) {
        return new SqlParseException(lex.input(), lex.tokenStartPosition(), msg);
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @return Excpetion.
     */
    private SqlParseException exceptionUnexpectedToken() {
        throw exception("Unexpected token: " + lex.token());
    }
}
