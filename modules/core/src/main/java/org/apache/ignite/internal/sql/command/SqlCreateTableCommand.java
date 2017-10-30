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
import static org.apache.ignite.internal.sql.SqlKeyword.KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.NOT;
import static org.apache.ignite.internal.sql.SqlKeyword.PRIMARY;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.isVaildIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * CREATE INDEX command.
 */
public class SqlCreateTableCommand implements SqlCommand {
    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** IF NOT EXISTS flag. */
    private boolean ifNotExists;

    /** Columns. */
    @GridToStringInclude
    private Collection<SqlColumn> cols;

    /** Primary key column names. */
    @GridToStringInclude
    private Collection<String> pkColNames;

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
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
    public Collection<SqlColumn> columns() {
        return cols != null ? cols : Collections.<SqlColumn>emptySet();
    }

    /**
     * @return PK column names.
     */
    public Collection<String> primaryKeyColumnNames() {
        return pkColNames != null ? pkColNames : Collections.<String>emptySet();
    }

    /**
     * @param col Column.
     * @return This instance.
     */
    private SqlCreateTableCommand addColumn(SqlColumn col) {
        if (cols == null)
            cols = new LinkedList<>();

        cols.add(col);

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        parseIfNotExists(lex);

        SqlQualifiedName tblQName = parseQualifiedIdentifier(lex, IF);

        schemaName = tblQName.schemaName();
        tblName = tblQName.name();

        parseColumnAndConstraintList(lex);

        return this;
    }

    /**
     * @param lex Lexer.
     */
    private void parseIfNotExists(SqlLexer lex) {
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
    private void parseColumnAndConstraintList(SqlLexer lex) {
        if (!lex.shift() || lex.tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT)
            throw errorUnexpectedToken(lex, "(");

        boolean last = false;

        while (!last) {
            parseColumnOrConstraint(lex);

            if (!lex.shift())
                throw errorUnexpectedToken(lex, ",", ")");

            switch (lex.tokenType()) {
                case COMMA:
                    break;

                case PARENTHESIS_RIGHT:
                    last = true;

                    break;

                default:
                    throw errorUnexpectedToken(lex, ",", ")");
            }
        }
    }

    /**
     * @param lex Lexer.
     */
    private void parseColumnOrConstraint(SqlLexer lex) {
        SqlParserToken next = lex.lookAhead();

        // TODO: Throw errors on unsupported features (keywords!!!).

        if (matchesKeyword(next, PRIMARY))
            parsePrimaryKeyConstraint(lex);
        else
            parseColumn(lex);
    }

    /**
     * @param lex Lexer.
     */
    private void parseColumn(SqlLexer lex) {
        // TODO: Throw on alraedy defined!

        // TODO
    }

    /**
     * @param lex Lexer.
     */
    private void parsePrimaryKeyConstraint(SqlLexer lex) {
        // TODO: throw on already defined!

        skipIfMatchesKeyword(lex, PRIMARY);
        skipIfMatchesKeyword(lex, KEY);

        // TODO: Column names!
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlCreateTableCommand.class, this);
    }
}
