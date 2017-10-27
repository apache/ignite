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
import org.apache.ignite.internal.sql.command.SqlIndexColumn;
import org.apache.ignite.internal.sql.command.SqlQualifiedName;

import static org.apache.ignite.internal.sql.SqlKeyword.ASC;
import static org.apache.ignite.internal.sql.SqlKeyword.CREATE;
import static org.apache.ignite.internal.sql.SqlKeyword.DESC;
import static org.apache.ignite.internal.sql.SqlKeyword.DROP;
import static org.apache.ignite.internal.sql.SqlKeyword.EXISTS;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlKeyword.INDEX;
import static org.apache.ignite.internal.sql.SqlKeyword.NOT;
import static org.apache.ignite.internal.sql.SqlKeyword.ON;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;

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
            if (matchesKeyword(INDEX))
                return processCreateIndex();
        }

        throw errorUnexpectedToken(lex, INDEX);
    }

    /**
     * Process CREATE INDEX command.
     *
     * @return Command.
     */
    private SqlCreateIndexCommand processCreateIndex() {
        if (!lex.shift())
            throw errorUnexpectedToken(lex, "[name]", IF);

        SqlCreateIndexCommand cmd = new SqlCreateIndexCommand();

        // Process IF NOT EXISTS.
        if (matchesKeyword(IF)) {
            skipIfMatchesKeyword(NOT);
            skipIfMatchesKeyword(EXISTS);

            cmd.ifNotExists(true);

            if (!lex.shift())
                throw errorUnexpectedToken(lex, "[name]");
        }

        // Process index name.
        String idxName = processName();

        cmd.indexName(idxName);

        // Skip ON.
        skipIfMatchesKeyword(ON);

        // Process table name.
        if (!lex.shift())
            throw errorUnexpectedToken(lex, "[qualified name]");

        SqlQualifiedName tblQName = processQualifiedName();

        cmd.schemaName(tblQName.schemaName());
        cmd.tableName(tblQName.name());

        // Process column list.
        if (!lex.shift())
            throw errorUnexpectedToken(lex, "(");

        processIndexColumnList(cmd);

        return cmd;
    }

    /**
     * Process index column list.
     *
     * @param cmd Command.
     */
    private void processIndexColumnList(SqlCreateIndexCommand cmd) {
        if (lex.tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT )
            throw errorUnexpectedToken(lex, "(");

        boolean lastCol = false;

        while (!lastCol) {
            if (!lex.shift())
                throw errorUnexpectedToken(lex, "[name]");

            SqlIndexColumn col = processIndexColumn();

            cmd.addColumn(col);

            if (!lex.shift())
                throw errorUnexpectedToken(lex, ",", ")");

            switch (lex.tokenType()) {
                case COMMA:
                    break;

                case PARENTHESIS_RIGHT:
                    lastCol = true;

                    break;

                default:
                    throw errorUnexpectedToken(lex, ",", ")");
            }
        }
    }

    /**
     * Process index column.
     *
     * @return Index column.
     */
    private SqlIndexColumn processIndexColumn() {
        String name = processName();
        boolean desc = false;

        SqlLexer forkerLex = lex.fork();

        if (forkerLex.shift() && (matchesKeyword(forkerLex, ASC) || matchesKeyword(forkerLex, DESC))) {
            lex.shift();

            if (matchesKeyword(DESC))
                desc = true;
        }

        return new SqlIndexColumn().name(name).descending(desc);
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

        throw errorUnexpectedToken(lex, INDEX);
    }

    /**
     * Process DROP INDEX command.
     *
     * @return Command.
     */
    private SqlDropIndexCommand processDropIndex() {
        if (!lex.shift())
            throw errorUnexpectedToken(lex, "[qualified name]", IF);

        SqlDropIndexCommand cmd = new SqlDropIndexCommand();

        if (matchesKeyword(IF)) {
            skipIfMatchesKeyword(EXISTS);

            cmd.ifExists(true);

            if (!lex.shift())
                throw errorUnexpectedToken(lex, "[qualified name]");
        }

        SqlQualifiedName idxQName = processQualifiedName();

        cmd.schemaName(idxQName.schemaName());
        cmd.indexName(idxQName.name());

        return cmd;
    }

    /**
     * Process qualified name.
     *
     * @return Qualified name.
     */
    private SqlQualifiedName processQualifiedName() {
        if (isIdentifier(lex.tokenType())) {
            SqlQualifiedName res = new SqlQualifiedName();

            String first = lex.token();

            SqlLexer forkedLex = lex.fork();

            if (forkedLex.shift() && forkedLex.tokenType() == SqlLexerTokenType.DOT) {
                lex.shift(); // Skip dot.

                if (!lex.shift())
                    throw errorUnexpectedToken(lex, "[name]");

                String second = processName();

                return res.schemaName(first).name(second);
            }
            else
                return res.name(first);
        }

        throw errorUnexpectedToken(lex, "[qualified name]");
    }

    /**
     * Process name.
     *
     * @return Name.
     */
    private String processName() {
        if (isIdentifier(lex.tokenType()))
            return lex.token();
        else
            throw errorUnexpectedToken(lex, "[name]");
    }

    /**
     * @param tokenTyp Ttoken type.
     * @return {@code True} if we are standing on possible identifier.
     */
    private boolean isIdentifier(SqlLexerTokenType tokenTyp) {
        return tokenTyp == SqlLexerTokenType.DEFAULT || tokenTyp == SqlLexerTokenType.QUOTED;
    }

    /**
     * Check if current lexer token matches expected.
     *
     * @param expKeyword Expected keyword.
     * @return {@code True} if matches.
     */
    private boolean matchesKeyword(String expKeyword) {
        return matchesKeyword(lex, expKeyword);
    }

    /**
     * Check if current lexer token matches expected.
     *
     * @param lex Lexer.
     * @param expKeyword Expected keyword.
     * @return {@code True} if matches.
     */
    private static boolean matchesKeyword(SqlLexer lex, String expKeyword) {
        if (lex.tokenType() != SqlLexerTokenType.DEFAULT)
            return false;

        String token = lex.token();

        return expKeyword.equals(token);
    }

    /**
     * Skip token if it matches expected keyword.
     *
     * @param expKeyword Expected keyword.
     */
    private void skipIfMatchesKeyword(String expKeyword) {
        if (lex.shift() && matchesKeyword(expKeyword))
            return;

        throw errorUnexpectedToken(lex, expKeyword);
    }

    /**
     * @return Original SQL.
     */
    public String sql() {
        return lex.input();
    }
}
