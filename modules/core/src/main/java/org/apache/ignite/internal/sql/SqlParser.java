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
import org.apache.ignite.internal.sql.command.SqlCreateTableCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropTableCommand;

import static org.apache.ignite.internal.sql.SqlKeyword.CREATE;
import static org.apache.ignite.internal.sql.SqlKeyword.DROP;
import static org.apache.ignite.internal.sql.SqlKeyword.INDEX;
import static org.apache.ignite.internal.sql.SqlKeyword.SPATIAL;
import static org.apache.ignite.internal.sql.SqlKeyword.TABLE;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;

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
                            if (matchesKeyword(lex, CREATE))
                                cmd = processCreate();

                            break;

                        case 'D':
                            if (matchesKeyword(lex, DROP))
                                cmd = processDrop();

                            break;
                    }


                    if (cmd != null) {
                        // If there is something behind the command, this is a syntax error.
                        if (lex.shift() && lex.tokenType() != SqlLexerTokenType.SEMICOLON)
                            throw errorUnexpectedToken(lex);

                        return cmd;
                    }
                    else
                        throw errorUnexpectedToken(lex);

                case QUOTED:
                case MINUS:
                case DOT:
                case COMMA:
                case PARENTHESIS_LEFT:
                case PARENTHESIS_RIGHT:
                default:
                    throw errorUnexpectedToken(lex);
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
            SqlCommand cmd = null;

            if (matchesKeyword(lex, TABLE))
                cmd = new SqlCreateTableCommand();
            if (matchesKeyword(lex, INDEX))
                cmd = new SqlCreateIndexCommand();
            else if (matchesKeyword(lex, SPATIAL)) {
                if (lex.shift() && matchesKeyword(lex, INDEX))
                    cmd = new SqlCreateIndexCommand().spatial(true);
                else
                    throw errorUnexpectedToken(lex, INDEX);
            }

            if (cmd != null)
                return cmd.parse(lex);
        }

        throw errorUnexpectedToken(lex, INDEX, SPATIAL);
    }

    /**
     * Process DROP keyword.
     *
     * @return Command.
     */
    private SqlCommand processDrop() {
        if (lex.shift()) {
            SqlCommand cmd = null;

            if (matchesKeyword(lex, INDEX))
                cmd = new SqlDropIndexCommand();
            else if (matchesKeyword(lex, TABLE))
                cmd = new SqlDropTableCommand();

            if (cmd != null)
                return cmd.parse(lex);
        }

        throw errorUnexpectedToken(lex, INDEX);
    }
}
