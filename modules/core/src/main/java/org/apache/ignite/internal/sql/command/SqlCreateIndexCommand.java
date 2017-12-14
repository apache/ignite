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

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlLexerToken;
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import static org.apache.ignite.internal.sql.SqlKeyword.ASC;
import static org.apache.ignite.internal.sql.SqlKeyword.DESC;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlKeyword.INLINE_SIZE;
import static org.apache.ignite.internal.sql.SqlKeyword.ON;
import static org.apache.ignite.internal.sql.SqlKeyword.PARALLEL;
import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIfNotExists;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipKeyword;

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

    /** Spatial index flag. */
    private boolean spatial;

    /**
     * Parallelism level. <code>parallel=0</code> means that a default number
     * of cores will be used during index creation (e.g. 25% of available cores).
     */
    private int parallel;

    /** Columns. */
    @GridToStringInclude
    private Collection<SqlIndexColumn> cols;

    /** Column names. */
    @GridToStringExclude
    private Set<String> colNames;

    /** Inline size. Zero effectively disables inlining. */
    private int inlineSize = QueryIndex.DFLT_INLINE_SIZE;

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        this.schemaName = schemaName;
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
     * @param val Parallelism level.
     */
    public void parallel(int val) {
        parallel = val;
    }

    /**
     * @return Parallelism level.
     */
    public int parallel() {
        return parallel;
    }

    /**
     * @return Spatial index flag.
     */
    public boolean spatial() {
        return spatial;
    }

    /**
     * @return Inline size.
     */
    public int inlineSize() {
        return inlineSize;
    }

    /**
     * @param val Inline size.
     */
    public void inlineSize(int val) {
        inlineSize = val;
    }

    /**
     * @param spatial Spatial index flag.
     * @return This instance.
     */
    public SqlCreateIndexCommand spatial(boolean spatial) {
        this.spatial = spatial;

        return this;
    }

    /**
     * @return Columns.
     */
    public Collection<SqlIndexColumn> columns() {
        return cols != null ? cols : Collections.<SqlIndexColumn>emptySet();
    }

    private Set<String> parsedParams = new HashSet<>();

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        ifNotExists = parseIfNotExists(lex);

        idxName = parseIndexName(lex);

        skipKeyword(lex, ON);

        SqlQualifiedName tblQName = parseQualifiedIdentifier(lex);

        schemaName = tblQName.schemaName();
        tblName = tblQName.name();

        parseColumnList(lex);

        parseParameters(lex);

        return this;
    }

    /**
     * Pasrse index name.
     *
     * @param lex Lexer.
     * @return Index name.
     */
    private static @Nullable String parseIndexName(SqlLexer lex) {
        if (matchesKeyword(lex.lookAhead(), ON))
            return null;

        return parseIdentifier(lex, IF);
    }


    /**
     * @param lex Lexer.
     */
    private void parseColumnList(SqlLexer lex) {
        if (!lex.shift() || lex.tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT)
            throw errorUnexpectedToken(lex, "(");

        while (true) {
            parseIndexColumn(lex);

            if (skipCommaOrRightParenthesis(lex) == SqlLexerTokenType.PARENTHESIS_RIGHT)
                break;
        }
    }

    /**
     * @param lex Lexer.
     */
    private void parseIndexColumn(SqlLexer lex) {
        String name = parseIdentifier(lex);
        boolean desc = false;

        SqlLexerToken nextToken = lex.lookAhead();

        if (matchesKeyword(nextToken, ASC) || matchesKeyword(nextToken, DESC)) {
            lex.shift();

            if (matchesKeyword(lex, DESC))
                desc = true;
        }

        addColumn(lex, new SqlIndexColumn(name, desc));
    }

    /**
     * @param lex Lexer.
     * @param col Column.
     */
    private void addColumn(SqlLexer lex, SqlIndexColumn col) {
        if (cols == null) {
            cols = new LinkedList<>();
            colNames = new HashSet<>();
        }

        if (!colNames.add(col.name()))
            throw error(lex, "Column already defined: " + col.name());

        cols.add(col);
    }

    /**
     * Parses CREATE INDEX command properties.
     *
     * @param lex Lexer.
     */
    private void parseParameters(SqlLexer lex) {
        while (lex.tokenType() != SqlLexerTokenType.EOF) {

            if (!tryParseParallel(lex) &&
                !tryParseInlineSize(lex))

                throw errorUnexpectedToken(lex.currentToken());
        }
    }

    /** FIXME */
    private boolean tryParseParallel(final SqlLexer lex) {

        return SqlParserUtils.tryParseIntParam(lex, PARALLEL, parsedParams, false, new SqlParserUtils.Setter<Integer>() {

            @Override public void apply(Integer val, boolean isDflt, boolean isQuoted) {
                assert val != null;

                if (val < 0)
                    throw error(lex, "Illegal " + PARALLEL + " value. Should be positive: " + parallel);

                parallel(val);
            }
        });
    }

    /** FIXME */
    private boolean tryParseInlineSize(final SqlLexer lex) {

        return SqlParserUtils.tryParseIntParam(lex, INLINE_SIZE, parsedParams, true, new SqlParserUtils.Setter<Integer>() {

            @Override public void apply(Integer val, boolean isDflt, boolean isQuoted) {
                assert isDflt || val != null;

                if (val < 0)
                    throw error(lex, "Illegal " + INLINE_SIZE + " value. Should be positive: " + parallel);

                inlineSize(isDflt ? QueryIndex.DFLT_INLINE_SIZE : val);
            }
        });
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlCreateIndexCommand.class, this);
    }
}
