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

import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.sql.command.SqlAlterTableCommand;
import org.apache.ignite.internal.sql.command.SqlAlterUserCommand;
import org.apache.ignite.internal.sql.command.SqlBeginTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCommitTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlCreateUserCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropUserCommand;
import org.apache.ignite.internal.sql.command.SqlKillComputeTaskCommand;
import org.apache.ignite.internal.sql.command.SqlKillContinuousQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillScanQueryCommand;
import org.apache.ignite.internal.sql.command.SqlKillServiceCommand;
import org.apache.ignite.internal.sql.command.SqlKillTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlRollbackTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlSetStreamingCommand;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.sql.SqlKeyword.ALTER;
import static org.apache.ignite.internal.sql.SqlKeyword.BEGIN;
import static org.apache.ignite.internal.sql.SqlKeyword.COMMIT;
import static org.apache.ignite.internal.sql.SqlKeyword.COMPUTE;
import static org.apache.ignite.internal.sql.SqlKeyword.CONTINUOUS;
import static org.apache.ignite.internal.sql.SqlKeyword.COPY;
import static org.apache.ignite.internal.sql.SqlKeyword.CREATE;
import static org.apache.ignite.internal.sql.SqlKeyword.DROP;
import static org.apache.ignite.internal.sql.SqlKeyword.GRANT;
import static org.apache.ignite.internal.sql.SqlKeyword.HASH;
import static org.apache.ignite.internal.sql.SqlKeyword.HELP;
import static org.apache.ignite.internal.sql.SqlKeyword.INDEX;
import static org.apache.ignite.internal.sql.SqlKeyword.KILL;
import static org.apache.ignite.internal.sql.SqlKeyword.PRIMARY;
import static org.apache.ignite.internal.sql.SqlKeyword.QUERY;
import static org.apache.ignite.internal.sql.SqlKeyword.REVOKE;
import static org.apache.ignite.internal.sql.SqlKeyword.ROLLBACK;
import static org.apache.ignite.internal.sql.SqlKeyword.SCAN;
import static org.apache.ignite.internal.sql.SqlKeyword.SERVICE;
import static org.apache.ignite.internal.sql.SqlKeyword.SET;
import static org.apache.ignite.internal.sql.SqlKeyword.SHOW;
import static org.apache.ignite.internal.sql.SqlKeyword.SPATIAL;
import static org.apache.ignite.internal.sql.SqlKeyword.START;
import static org.apache.ignite.internal.sql.SqlKeyword.STREAMING;
import static org.apache.ignite.internal.sql.SqlKeyword.TABLE;
import static org.apache.ignite.internal.sql.SqlKeyword.TRANSACTION;
import static org.apache.ignite.internal.sql.SqlKeyword.UNIQUE;
import static org.apache.ignite.internal.sql.SqlKeyword.USER;
import static org.apache.ignite.internal.sql.SqlKeyword.WORK;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnsupportedIfMatchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesOptionalKeyword;

/**
 * SQL parser.
 */
public class SqlParser {
    /** Scheme name. */
    private final String schemaName;

    /** Lexer. */
    private final SqlLexer lex;

    /** Begin position of the last successfully parsed sql command. */
    private int lastCmdBeginPos;

    /** Position right after the end of the last successfully parsed sql command or {@code -1} if haven't parsed. */
    private int lastCmdEndPos = -1;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param sql Original SQL.
     */
    public SqlParser(@Nullable String schemaName, String sql) {
        this.schemaName = schemaName;

        lex = new SqlLexer(sql);
    }

    /**
     * Get next command.
     *
     * @return Command or {@code null} if end of script is reached.
     */
    public SqlCommand nextCommand() {
        SqlCommand cmd = nextCommand0();

        if (cmd != null) {
            if (cmd.schemaName() == null)
                cmd.schemaName(schemaName);
        }

        return cmd;
    }

    /**
     * Get next command.
     *
     * @return Command or {@code null} if end of script is reached.
     */
    private SqlCommand nextCommand0() {
        while (true) {
            if (!lex.shift()) {
                lastCmdEndPos = -1;

                return null;
            }

            switch (lex.tokenType()) {
                case SEMICOLON:
                    // Note: currently we don't use native parser for empty statements. But if we start, we need
                    // NoOp sql command, because we have to send empty results for the empty statements, not just to
                    // ignore them.

                    // Empty command, skip.
                    continue;

                case DEFAULT:
                    SqlCommand cmd = null;

                    int curCmdBegin = lex.tokenPosition();

                    switch (lex.token()) {
                        case BEGIN:
                            cmd = processBegin();

                            break;

                        case COMMIT:
                            cmd = processCommit();

                            break;

                        case CREATE:
                            cmd = processCreate();

                            break;

                        case DROP:
                            cmd = processDrop();

                            break;

                        case ROLLBACK:
                            cmd = processRollback();

                            break;

                        case START:
                            cmd = processStart();

                            break;

                        case COPY:
                            try {
                                cmd = processCopy();

                                break;
                            }
                            catch (SqlParseException e) {
                                throw new SqlStrictParseException(e);
                            }

                        case SET:
                            cmd = processSet();

                            break;

                        case ALTER:
                            cmd = processAlter();

                            break;

                        case KILL:
                            cmd = processKill();

                            break;

                        case HELP:
                            cmd = processHelp();

                            break;

                        case SHOW:
                            cmd = processShow();

                            break;

                        case GRANT:
                            cmd = processGrant();

                            break;

                        case REVOKE:
                            cmd = processRevoke();

                            break;
                    }

                    if (cmd != null) {
                        int curCmdEnd = lex.position();

                        // If there is something behind the command, this is a syntax error.
                        if (lex.shift() && lex.tokenType() != SqlLexerTokenType.SEMICOLON)
                            throw errorUnexpectedToken(lex);

                        lastCmdBeginPos = curCmdBegin;
                        lastCmdEndPos = curCmdEnd;

                        return cmd;
                    }
                    else
                        throw errorUnexpectedToken(lex, BEGIN, COMMIT, CREATE, DROP, ROLLBACK, COPY, SET, ALTER, START, KILL);

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
     * Process BEGIN keyword.
     *
     * @return Command.
     */
    private SqlCommand processBegin() {
        skipIfMatchesOptionalKeyword(lex, TRANSACTION);

        skipIfMatchesOptionalKeyword(lex, WORK);

        return new SqlBeginTransactionCommand();
    }

    /**
     * Process COMMIT keyword.
     *
     * @return Command.
     */
    private SqlCommand processCommit() {
        skipIfMatchesOptionalKeyword(lex, TRANSACTION);

        return new SqlCommitTransactionCommand();
    }

    /**
     * Process SET keyword.
     *
     * @return Command.
     */
    private SqlCommand processSet() {
        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT) {
            switch (lex.token()) {
                case STREAMING:
                    return new SqlSetStreamingCommand().parse(lex);
            }
        }

        throw errorUnexpectedToken(lex, STREAMING);
    }

    /**
     * Process KILL keyword.
     *
     * @return Command.
     */
    private SqlCommand processKill() {
        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT) {
            switch (lex.token()) {
                case QUERY:
                    return new SqlKillQueryCommand().parse(lex);

                case SCAN:
                    return new SqlKillScanQueryCommand().parse(lex);

                case COMPUTE:
                    return new SqlKillComputeTaskCommand().parse(lex);

                case CONTINUOUS:
                    return new SqlKillContinuousQueryCommand().parse(lex);

                case SERVICE:
                    return new SqlKillServiceCommand().parse(lex);

                case TRANSACTION:
                    return new SqlKillTransactionCommand().parse(lex);
            }
        }

        throw errorUnexpectedToken(lex, QUERY);
    }

    /**
     * Processes COPY command.
     *
     * @return The {@link SqlBulkLoadCommand} command.
     */
    private SqlCommand processCopy() {
        return new SqlBulkLoadCommand().parse(lex);
    }

    /**
     * Process CREATE keyword.
     *
     * @return Command.
     */
    private SqlCommand processCreate() {
        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT) {
            SqlCommand cmd = null;

            switch (lex.token()) {
                case INDEX:
                    cmd = new SqlCreateIndexCommand();

                    break;

                case SPATIAL:
                    if (lex.shift() && matchesKeyword(lex, INDEX))
                        cmd = new SqlCreateIndexCommand().spatial(true);
                    else
                        throw errorUnexpectedToken(lex, INDEX);

                    break;

                case USER:
                    cmd = new SqlCreateUserCommand();

                    break;

            }

            if (cmd != null)
                return cmd.parse(lex);

            errorUnsupportedIfMatchesKeyword(lex, HASH, PRIMARY, UNIQUE);
        }

        throw errorUnexpectedToken(lex, INDEX, SPATIAL, USER);
    }

    /**
     * Process DROP keyword.
     *
     * @return Command.
     */
    private SqlCommand processDrop() {
        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT) {
            SqlCommand cmd = null;

            switch (lex.token()) {
                case INDEX:
                    cmd = new SqlDropIndexCommand();

                    break;

                case USER:
                    cmd = new SqlDropUserCommand();

                    break;
            }

            if (cmd != null)
                return cmd.parse(lex);
        }

        throw errorUnexpectedToken(lex, INDEX, USER);
    }

    /**
     * Process ROLLBACK keyword.
     *
     * @return Command.
     */
    private SqlCommand processRollback() {
        skipIfMatchesOptionalKeyword(lex, TRANSACTION);

        return new SqlRollbackTransactionCommand();
    }

    /**
     * Process START keyword.
     *
     * @return Command.
     */
    private SqlCommand processStart() {
        skipIfMatchesKeyword(lex, TRANSACTION);

        return new SqlBeginTransactionCommand();
    }

    /**
     * Process ALTER keyword.
     *
     * @return Command.
     */
    private SqlCommand processAlter() {
        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT) {
            SqlCommand cmd = null;

            switch (lex.token()) {
                case TABLE:
                    cmd = new SqlAlterTableCommand();

                    break;

                case USER:
                    cmd = new SqlAlterUserCommand();

                    break;
            }

            if (cmd != null)
                return cmd.parse(lex);
        }

        throw errorUnexpectedToken(lex, TABLE, USER);
    }

    /**
     * Process HELP keyword.
     *
     * @return Command.
     */
    private SqlCommand processHelp() {
        throw new SqlStrictParseException("HELP command is not supported", IgniteQueryErrorCode.UNSUPPORTED_OPERATION,
            SqlParserUtils.errorUnsupported(lex));
    }

    /**
     * Process SHOW keyword.
     *
     * @return Command.
     */
    private SqlCommand processShow() {
        throw new SqlStrictParseException("SHOW command is not supported", IgniteQueryErrorCode.UNSUPPORTED_OPERATION,
            SqlParserUtils.errorUnsupported(lex));
    }

    /**
     * Process GRANT keyword.
     *
     * @return Command.
     */
    private SqlCommand processGrant() {
        throw new SqlStrictParseException("GRANT command is not supported", IgniteQueryErrorCode.UNSUPPORTED_OPERATION,
            SqlParserUtils.errorUnsupported(lex));
    }

    /**
     * Process REVOKE keyword.
     *
     * @return Command.
     */
    private SqlCommand processRevoke() {
        throw new SqlStrictParseException("REVOKE command is not supported", IgniteQueryErrorCode.UNSUPPORTED_OPERATION,
            SqlParserUtils.errorUnsupported(lex));
    }

    /**
     * Not yet parsed part of the sql query. Result is invalid if parsing error was thrown.
     */
    public String remainingSql() {
        if (lex.eod())
            return null;

        return lex.sql().substring(lex.position());
    }

    /**
     * Last successfully parsed sql statement. It corresponds to the last command returned by {@link #nextCommand()}.
     */
    public String lastCommandSql() {
        if (lastCmdEndPos < 0)
            return null;

        return lex.sql().substring(lastCmdBeginPos, lastCmdEndPos);
    }
}
