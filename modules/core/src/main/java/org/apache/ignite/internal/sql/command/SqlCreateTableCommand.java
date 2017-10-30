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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import static org.apache.ignite.internal.sql.SqlKeyword.BIGINT;
import static org.apache.ignite.internal.sql.SqlKeyword.BIT;
import static org.apache.ignite.internal.sql.SqlKeyword.BOOL;
import static org.apache.ignite.internal.sql.SqlKeyword.BOOLEAN;
import static org.apache.ignite.internal.sql.SqlKeyword.CHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.CHARACTER;
import static org.apache.ignite.internal.sql.SqlKeyword.DATE;
import static org.apache.ignite.internal.sql.SqlKeyword.DATETIME;
import static org.apache.ignite.internal.sql.SqlKeyword.DEC;
import static org.apache.ignite.internal.sql.SqlKeyword.DECIMAL;
import static org.apache.ignite.internal.sql.SqlKeyword.DOUBLE;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT4;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT8;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlKeyword.INT;
import static org.apache.ignite.internal.sql.SqlKeyword.INT2;
import static org.apache.ignite.internal.sql.SqlKeyword.INT4;
import static org.apache.ignite.internal.sql.SqlKeyword.INT8;
import static org.apache.ignite.internal.sql.SqlKeyword.INTEGER;
import static org.apache.ignite.internal.sql.SqlKeyword.KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.LONGVARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.MEDIUMINT;
import static org.apache.ignite.internal.sql.SqlKeyword.NCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.NUMBER;
import static org.apache.ignite.internal.sql.SqlKeyword.NUMERIC;
import static org.apache.ignite.internal.sql.SqlKeyword.NVARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.NVARCHAR2;
import static org.apache.ignite.internal.sql.SqlKeyword.PRECISION;
import static org.apache.ignite.internal.sql.SqlKeyword.PRIMARY;
import static org.apache.ignite.internal.sql.SqlKeyword.REAL;
import static org.apache.ignite.internal.sql.SqlKeyword.SIGNED;
import static org.apache.ignite.internal.sql.SqlKeyword.SMALLDATETIME;
import static org.apache.ignite.internal.sql.SqlKeyword.SMALLINT;
import static org.apache.ignite.internal.sql.SqlKeyword.TIME;
import static org.apache.ignite.internal.sql.SqlKeyword.TIMESTAMP;
import static org.apache.ignite.internal.sql.SqlKeyword.TINYINT;
import static org.apache.ignite.internal.sql.SqlKeyword.UUID;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR2;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR_CASESENSITIVE;
import static org.apache.ignite.internal.sql.SqlKeyword.YEAR;
import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIfNotExists;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseInt;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatches;
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

    /** Column names. */
    private Set<String> colNames;

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
        if (cols == null) {
            cols = new LinkedList<>();
            colNames = new HashSet<>();
        }

        cols.add(col);
        colNames.add(col.name());

        return this;
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        ifNotExists = parseIfNotExists(lex);

        SqlQualifiedName tblQName = parseQualifiedIdentifier(lex, IF);

        schemaName = tblQName.schemaName();
        tblName = tblQName.name();

        parseColumnAndConstraintList(lex);

        return this;
    }

    /*
     * @param lex Lexer.
     */
    private void parseColumnAndConstraintList(SqlLexer lex) {
        if (!lex.shift() || lex.tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT)
            throw errorUnexpectedToken(lex, "(");

        while (true) {
            parseColumnOrConstraint(lex);

            if (skipCommaOrRightParenthesis(lex))
                break;
        }
    }

    /**
     * @param lex Lexer.
     */
    private void parseColumnOrConstraint(SqlLexer lex) {
        SqlParserToken next = lex.lookAhead();

        if (next.tokenType() == SqlLexerTokenType.EOF)
            throw errorUnexpectedToken(lex, PRIMARY, "[column definition]");

        if (matchesKeyword(next, PRIMARY))
            parsePrimaryKeyConstraint(lex);
        else
            parseColumn(lex);
    }

    /**
     * @param lex Lexer.
     */
    private void parseColumn(SqlLexer lex) {
        String name = parseIdentifier(lex);

        if (colNames != null && colNames.contains(name))
            throw error(lex, "Column already defined: " + name);

        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT) {
            SqlColumn col = null;

            switch (lex.token()) {
                case BIT:
                case BOOL:
                case BOOLEAN:
                    col = new SqlColumn(name, SqlColumnType.BOOLEAN);

                    break;

                case TINYINT:
                    col = new SqlColumn(name, SqlColumnType.BYTE);

                    break;

                case INT2:
                case SMALLINT:
                case YEAR:
                    col = new SqlColumn(name, SqlColumnType.SHORT);

                    break;

                case INT:
                case INT4:
                case INTEGER:
                case MEDIUMINT:
                case SIGNED:
                    col = new SqlColumn(name, SqlColumnType.INT);

                    break;

                case BIGINT:
                case INT8:
                    col = new SqlColumn(name, SqlColumnType.LONG);

                    break;

                case FLOAT4:
                case REAL:
                    col = new SqlColumn(name, SqlColumnType.FLOAT);

                    break;

                case DOUBLE: {
                    SqlParserToken next = lex.lookAhead();

                    if (matchesKeyword(next, PRECISION))
                        lex.shift();

                    col = new SqlColumn(name, SqlColumnType.DOUBLE);

                    break;
                }

                case FLOAT:
                case FLOAT8:
                    col = new SqlColumn(name, SqlColumnType.DOUBLE);

                    break;

                case DEC:
                case DECIMAL:
                case NUMBER:
                case NUMERIC: {
                    skipIfMatches(lex, SqlLexerTokenType.PARENTHESIS_LEFT);

                    int scale = parseInt(lex);
                    int precision = 0;

                    if (!skipCommaOrRightParenthesis(lex)) {
                        precision = parseInt(lex);

                        skipIfMatches(lex, SqlLexerTokenType.PARENTHESIS_RIGHT);
                    }

                    col = new SqlColumn(name, SqlColumnType.DECIMAL, scale, precision);

                    break;
                }

                case CHAR:
                case CHARACTER:
                case NCHAR: {
                    int precision = parseStringPrecision(lex);

                    col = new SqlColumn(name, SqlColumnType.CHAR, precision);

                    break;
                }

                case LONGVARCHAR:
                case NVARCHAR:
                case NVARCHAR2:
                case VARCHAR:
                case VARCHAR2:
                case VARCHAR_CASESENSITIVE: {
                    int precision = parseStringPrecision(lex);

                    col = new SqlColumn(name, SqlColumnType.VARCHAR, precision);

                    break;
                }

                case DATE:
                    col = new SqlColumn(name, SqlColumnType.DATE);

                    break;

                case TIME:
                    col = new SqlColumn(name, SqlColumnType.TIME);

                    break;

                case DATETIME:
                case SMALLDATETIME:
                case TIMESTAMP:
                    col = new SqlColumn(name, SqlColumnType.TIMESTAMP);

                    break;

                case UUID:
                    col = new SqlColumn(name, SqlColumnType.UUID);

                    break;
            }

            if (col != null) {
                addColumn(col);

                SqlParserToken next = lex.lookAhead();

                if (matchesKeyword(next, PRIMARY)) {
                    if (pkColNames != null)
                        throw error(lex, "PRIMARY KEY is already defined.");

                    pkColNames = new HashSet<>();

                    pkColNames.add(col.name());

                    lex.shift();

                    skipIfMatchesKeyword(lex, KEY);
                }

                return;
            }
        }

        throw errorUnexpectedToken(lex, "[column_type]");
    }

    /**
     * @param lex Lexer.
     */
    private void parsePrimaryKeyConstraint(SqlLexer lex) {
        if (pkColNames != null)
            throw error(lex, "PRIMARY KEY is already defined.");

        pkColNames = new HashSet<>();

        skipIfMatchesKeyword(lex, PRIMARY);
        skipIfMatchesKeyword(lex, KEY);

        skipIfMatches(lex, SqlLexerTokenType.PARENTHESIS_LEFT);

        while (true) {
            String pkColName = parseIdentifier(lex);

            if (!pkColNames.add(pkColName))
                throw error(lex, "Duplicate PK column name: " + pkColName);

            if (skipCommaOrRightParenthesis(lex))
                break;
        }
    }

    /**
     * Parse precision for CHAR and VARCHAR types.
     *
     * @param lex Lexer.
     * @return Precision.
     */
    private static int parseStringPrecision(SqlLexer lex) {
        SqlParserToken next = lex.lookAhead();

        int res = Integer.MAX_VALUE;

        if (next.tokenType() == SqlLexerTokenType.PARENTHESIS_LEFT) {
            lex.shift();

            res = parseInt(lex);

            skipIfMatches(lex, SqlLexerTokenType.PARENTHESIS_RIGHT);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlCreateTableCommand.class, this);
    }
}
