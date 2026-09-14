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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerToken;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

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
import static org.apache.ignite.internal.sql.SqlParserUtils.parseInt;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseString;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * CREATE INDEX command.  // modify@byron support Fulltext Index
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
    
    /** full text index flag. */
    private boolean fulltext;    

    /** Lucene index command */
    private String luceneIndexOptions;

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

    /**
     * Default constructor.
     */
    public SqlCreateIndexCommand() {
    }

    /**
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @param ifNotExists "If not exists" clause.
     * @param cols Indexed columns.
     * @param spatial Spatial flag.
     * @param parallel Count of threads to rebuild.
     * @param inlineSize Inline size.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public SqlCreateIndexCommand(
        String schemaName,
        @Nullable String tblName,
        String idxName,
        boolean ifNotExists,
        Collection<SqlIndexColumn> cols,
        boolean spatial,
        int parallel,
        int inlineSize
    ) {
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.idxName = idxName;
        this.ifNotExists = ifNotExists;
        this.spatial = spatial;
        this.parallel = parallel;
        this.inlineSize = inlineSize;
        this.cols = cols;

        colNames = new HashSet<>();

        for (SqlIndexColumn col : cols) {
            if (!colNames.add(col.name()))
                throw new IllegalArgumentException("Column already defined: " + col.name());
        }
    }

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
     * @return full text index flag.
     */
    public boolean fulltext() {
        return fulltext;
    }

    /**
     *
     * @return lucene index options
     */
    public String luceneIndexOptions(){
        return luceneIndexOptions;
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

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        ifNotExists = parseIfNotExists(lex);

        idxName = parseIndexName(lex);

        skipIfMatchesKeyword(lex, ON);

        SqlQualifiedName tblQName = parseQualifiedIdentifier(lex);

        schemaName = tblQName.schemaName();
        tblName = tblQName.name();

        parseColumnList(lex);

        parseIndexProperties(lex);

        return this;
    }

    /**
     * Parse index name.
     *
     * @param lex Lexer.
     * @return Index name.
     */
    @Nullable private static String parseIndexName(SqlLexer lex) {
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

            if (skipCommaOrRightParenthesis(lex))
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
    private void parseIndexProperties(SqlLexer lex) {
        Set<String> foundProps = new HashSet<>();

        while (true) {
            SqlLexerToken token = lex.lookAhead();

            if (token.tokenType() != SqlLexerTokenType.DEFAULT)
                break;

            switch (token.token()) {
                case PARALLEL:
                    parallel = getIntProperty(lex, PARALLEL, foundProps);

                    if (parallel < 0)
                        throw error(lex, "Illegal " + PARALLEL + " value. Should be positive: " + parallel);

                    break;

                case INLINE_SIZE:
                    inlineSize = getIntProperty(lex, INLINE_SIZE, foundProps);

                    if (inlineSize < 0)
                        throw error(lex, "Illegal " + INLINE_SIZE +
                            " value. Should be positive: " + inlineSize);

                    break;
                    
                case "FULLTEXT":
                    fulltext = true;

                    luceneIndexOptions = getStringProperty(lex, "FULLTEXT", foundProps);

                    //disabled
                    inlineSize = 0;
                    

                    //now make some validations here
                    if (spatial){
                        throw error(lex, "Spatial flag is not allowed for FULLTEXT index");
                    }


                default:
                    return;
            }
        }
    }

    /**
     * Parses <code>Integer</code> property by its keyword.
     * @param lex Lexer.
     * @param keyword Keyword.
     * @param foundProps Set of properties to check if one has already been found in SQL clause.
     * @return parsed value;
     */
    private Integer getIntProperty(SqlLexer lex, String keyword, Set<String> foundProps) {
        if (foundProps.contains(keyword))
            throw error(lex, "Only one " + keyword + " clause may be specified.");

        foundProps.add(keyword);

        lex.shift();

        return parseInt(lex);
    }

    private String getStringProperty(SqlLexer lex, String keyword, Set<String> foundProps) {
        if (foundProps.contains(keyword))
            throw error(lex, "Only one " + keyword + " clause may be specified.");

        foundProps.add(keyword);

        lex.shift();

        return parseString(lex);
    }
    
    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlCreateIndexCommand.class, this);
    }
}
