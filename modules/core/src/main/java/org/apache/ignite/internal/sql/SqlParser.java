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
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.sql.SqlKeyword.CREATE;
import static org.apache.ignite.internal.sql.SqlKeyword.DROP;
import static org.apache.ignite.internal.sql.SqlKeyword.EXISTS;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlKeyword.INDEX;

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
                    SqlCommand cmd = null;

                    switch (lex.tokenFirstChar()) {
                        case 'C':
                            if (matchesKeyword(CREATE))
                                cmd = processCreate();

                            break;

                        case 'D':
                            if (matchesKeyword(DROP))
                                cmd = processDrop();

                            break;
                    }


                    if (cmd != null) {
                        // If there is something behind the command, this is a syntax error.
                        if (lex.shift() && lex.tokenType() != SqlLexerTokenType.SEMICOLON)
                            throw unexpectedToken();

                        return cmd;
                    }
                    else
                        throw unexpectedToken();

                case QUOTED:
                case MINUS:
                case DOT:
                case COMMA:
                case PARENTHESIS_LEFT:
                case PARENTHESIS_RIGHT:
                default:
                    throw unexpectedToken();
            }
        }
    }

    /**
     * Process CREATE keyword.
     *
     * @return Command.
     */
    private SqlCommand processCreate() {
        if (lex.shift()) {
            if (matchesKeyword(INDEX))
                return processCreateIndex();
        }

        throw unexpectedToken(INDEX);
    }

    /**
     * Process CREATE INDEX command.
     *
     * @return Command.
     */
    private SqlCreateIndexCommand processCreateIndex() {
        // TODO

        return null;
    }

    /**
     * Process DROP keyword.
     *
     * @return Command.
     */
    private SqlCommand processDrop() {
        if (lex.shift()) {
            if (matchesKeyword(INDEX))
                return processDropIndex();
        }

        throw unexpectedToken(INDEX);
    }

    /**
     * Process DROP INDEX command.
     *
     * @return Command.
     */
    private SqlDropIndexCommand processDropIndex() {
        if (lex.shift()) {
            SqlDropIndexCommand res = new SqlDropIndexCommand();

            if (matchesKeyword(IF)) {
                skipIfMatchesKeyword(EXISTS);

                res.ifExists(true);
            }

            // TODO: Read qualified index name!
            if (lex.tokenType() == SqlLexerTokenType.DEFAULT || lex.tokenType() == SqlLexerTokenType.QUOTED)
                res.indexName(lex.token());
            else
                throw unexpectedToken("[index name]");
        }

        throw unexpectedToken("[index name]", IF);
    }

    /**
     * Skip token if it matches expected keyword.
     *
     * @param expKeyword Expected keyword.
     */
    private void skipIfMatchesKeyword(String expKeyword) {
        if (lex.shift() && matchesKeyword(expKeyword))
            return;

        throw unexpectedToken(expKeyword);
    }

    /**
     * Check if current lexer token matches expected.
     *
     * @param expKeyword Expected keyword.
     * @return {@code True} if matches.
     */
    private boolean matchesKeyword(String expKeyword) {
        if (lex.tokenType() != SqlLexerTokenType.DEFAULT)
            return false;

        String token = lex.token();

        return expKeyword.equals(token);
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
     * @param expTokens Expected tokens (if any).
     * @return Exception.
     */
    private SqlParseException unexpectedToken(String... expTokens) {
        String token = lex.token();

        StringBuilder msg = new StringBuilder(
            token == null ? "Unexpected end of command" : "Unexpected token: " + token);

        if (!F.isEmpty(expTokens)) {
            msg.append(" (expected: ");

            boolean first = true;

            for (String expToken : expTokens) {
                if (first)
                    first = false;
                else
                    msg.append(", ");

                msg.append(expToken);
            }

            msg.append(")");
        }

        throw exception(msg.toString());
    }
}
