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

package org.apache.ignite.internal.sql.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.sql.SqlKeyword;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerToken;
import org.apache.ignite.internal.sql.SqlLexerTokenType;

import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;

/**
 * Base class for statistics related commands.
 */
public abstract class SqlStatisticsCommands implements SqlCommand {
    /** Schema name. */
    private String schemaName;

    /** Targets to process. */
    protected Collection<StatisticsTarget> targets = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Analyze targets.
     */
    public Collection<StatisticsTarget> targets() {
        return targets;
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        while (true) {

            SqlQualifiedName tblQName = parseQualifiedIdentifier(lex);

            String[] cols = parseColumnList(lex, false);

            targets.add(new StatisticsTarget(tblQName.schemaName(), tblQName.name(), cols));

            if (tryEnd(lex))
                return this;
        }
    }

    /**
     * Test if it is the end of command.
     *
     * @param lex Sql lexer.
     * @return {@code true} if end of command found, {@code false} - otherwise.
     */
    protected boolean tryEnd(SqlLexer lex) {
        return !lex.shift() || lex.tokenType() == SqlLexerTokenType.SEMICOLON;
    }

    /**
     * @param lex Lexer.
     * @param allowParams If {@code true} - allow params instead columns list.
     */
    protected String[] parseColumnList(SqlLexer lex, boolean allowParams) {
        SqlLexerToken nextTok = lex.lookAhead();
        if (nextTok.token() == null || nextTok.tokenType() == SqlLexerTokenType.SEMICOLON
            || nextTok.tokenType() == SqlLexerTokenType.COMMA)
            return null;

        if (allowParams && matchesKeyword(nextTok, SqlKeyword.WITH))
            return null;

        lex.shift();

        if (lex.tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT)
            throw errorUnexpectedToken(lex, "(");

        Set<String> res = new HashSet<>();

        while (true) {
            parseColumn(lex, res);

            if (skipCommaOrRightParenthesis(lex))
                break;
        }
        return (res.isEmpty()) ? null : res.toArray(new String[0]);
    }

    /**
     * Parse column name.
     *
     * @param lex Lexer.
     */
    private void parseColumn(SqlLexer lex, Set<String> cols) {
        String name = parseIdentifier(lex);
        if (!cols.add(name))
            throw error(lex, "Column " + name + " already defined.");
    }
}
