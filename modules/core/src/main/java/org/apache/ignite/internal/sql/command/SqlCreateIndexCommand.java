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

import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlParserToken;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

import static org.apache.ignite.internal.sql.SqlKeyword.ASC;
import static org.apache.ignite.internal.sql.SqlKeyword.DESC;
import static org.apache.ignite.internal.sql.SqlKeyword.EXISTS;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlKeyword.NOT;
import static org.apache.ignite.internal.sql.SqlKeyword.ON;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * CREATE INDEX command.
 */
public class SqlCreateIndexCommand implements SqlCommand {
    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Index name. */
    private String idxName;

    /** IF NOT EXISTS flag. */
    private boolean ifNotExists;

    /** Columns. */
    @GridToStringInclude
    private Collection<SqlIndexColumn> cols;

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     * @return This instance.
     */
    public SqlCreateIndexCommand schemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Index name.
     */
    public String indexName() {
        return idxName;
    }

    /**
     * @return IF NOT EXISTS flag.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @return Columns.
     */
    public Collection<SqlIndexColumn> columns() {
        return cols != null ? cols : Collections.<SqlIndexColumn>emptySet();
    }

    /**
     * @param col Column.
     * @return This instance.
     */
    private SqlCreateIndexCommand addColumn(SqlIndexColumn col) {
        if (cols == null)
            cols = new LinkedList<>();

        cols.add(col);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        processCreateIndexIfExists(lex);

        idxName = parseIdentifier(lex, IF);

        skipIfMatchesKeyword(lex, ON);

        SqlQualifiedName tblQName = parseQualifiedIdentifier(lex);

        schemaName = tblQName.schemaName();
        tblName = tblQName.name();

        parseIndexColumnList(lex);

        return this;
    }

    /**
     * @param lex Lexer.
     */
    private void processCreateIndexIfExists(SqlLexer lex) {
        SqlParserToken token = lex.lookAhead();

        if (token != null && matchesKeyword(token, IF)) {
            lex.shift();

            skipIfMatchesKeyword(lex, NOT);
            skipIfMatchesKeyword(lex, EXISTS);

            ifNotExists = true;
        }
    }

    /*
     * @param lex Lexer.
     */
    private void parseIndexColumnList(SqlLexer lex) {
        if (!lex.shift() || lex.tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT)
            throw errorUnexpectedToken(lex, "(");

        boolean lastCol = false;

        while (!lastCol) {
            SqlIndexColumn col = perseIndexColumn(lex);

            addColumn(col);

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
     * @param lex Lexer.
     * @return Index column.
     */
    private SqlIndexColumn perseIndexColumn(SqlLexer lex) {
        String name = parseIdentifier(lex);
        boolean desc = false;

        SqlParserToken nextToken = lex.lookAhead();

        if (nextToken != null && (matchesKeyword(nextToken, ASC) || matchesKeyword(nextToken, DESC))) {
            lex.shift();

            if (matchesKeyword(lex, DESC))
                desc = true;
        }

        return new SqlIndexColumn(name, desc);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlCreateIndexCommand.class, this);
    }
}
