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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.bulkload.BulkLoadFormat;
import org.apache.ignite.internal.processors.bulkload.BulkLoadParameters;
import org.apache.ignite.internal.sql.SqlKeyword;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseInt;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatches;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * A parser for a COPY command (called 'bulk load' in the code, since word 'copy' is too generic).
 */
public class SqlBulkLoadCommand implements SqlCommand {

    /** Local file name to send from client to server. */
    private String localFileName;

    /** Schema name + table name. */
    private SqlQualifiedName tblQName;

    /** User-specified list of columns. */
    private List<String> columns;

    /** File format. */
    private BulkLoadFormat inputFormat;

    /** Batch size (size of portion of a file sent in each sub-request). */
    private Integer batchSize;

    /**
     * Parses the command.
     *
     * @param lex The lexer.
     * @return The parsed command object.
     */
    @Override public SqlCommand parse(SqlLexer lex) {
        skipIfMatchesKeyword(lex, SqlKeyword.FROM); // COPY keyword is already parsed

        parseFileName(lex);

        parseTableName(lex);

        parseColumns(lex);

        parseFormat(lex);

        parseParameters(lex);

        return this;
    }

    /**
     * Parses the file name.
     *
     * @param lex The lexer.
     */
    private void parseFileName(SqlLexer lex) {
        localFileName = parseIdentifier(lex);
    }

    /**
     * Parses the schema and table names.
     *
     * @param lex The lexer.
     */
    private void parseTableName(SqlLexer lex) {
        skipIfMatchesKeyword(lex, SqlKeyword.INTO);

        tblQName = parseQualifiedIdentifier(lex);
    }

    /**
     * Parses the list of columns.
     *
     * @param lex The lexer.
     */
    private void parseColumns(SqlLexer lex) {
        skipIfMatches(lex, SqlLexerTokenType.PARENTHESIS_LEFT);

        columns = new ArrayList<>();

        do {
            columns.add(parseColumn(lex));
        }
        while (!skipCommaOrRightParenthesis(lex));
    }

    /**
     * Parses column clause.
     *
     * @param lex The lexer.
     * @return The column name.
     */
    private String parseColumn(SqlLexer lex) {
        String name = parseIdentifier(lex);

        return name;
    }

    /**
     * Parses the format clause.
     *
     * @param lex The lexer.
     */
    private void parseFormat(SqlLexer lex) {
        skipIfMatchesKeyword(lex, SqlKeyword.FORMAT);

        String name = parseIdentifier(lex);

        try {
            inputFormat = BulkLoadFormat.createFormatFor(name);
        } catch (IgniteCheckedException e) {
            throw error(lex, "Unknown format name: " + name + ". Currently supported formats are: "
                + BulkLoadFormat.formatNames());
        }
    }

    /**
     * Parses the optional parameters.
     *
     * @param lex The lexer.
     */
    private void parseParameters(SqlLexer lex) {
        while (lex.lookAhead().tokenType() == SqlLexerTokenType.DEFAULT) {
            switch (lex.lookAhead().token()) {
                case SqlKeyword.BATCH_SIZE:
                    lex.shift();

                    int sz = parseInt(lex);

                    if (sz < BulkLoadParameters.MIN_BATCH_SIZE || sz > BulkLoadParameters.MAX_BATCH_SIZE)
                        throw error(lex, "Batch size should be within [" +
                            BulkLoadParameters.MIN_BATCH_SIZE + ".." + BulkLoadParameters.MAX_BATCH_SIZE + "]: " + sz);

                    batchSize = sz;

                    break;

                default:
                    return;
            }
        }
    }

    /**
     * Returns the schemaName.
     *
     * @return schemaName.
     */
    public String schemaName() {
        return tblQName.schemaName();
    }

    @Override public void schemaName(String schemaName) {
        this.tblQName.schemaName(schemaName);
    }

    /**
     * Returns the table name.
     *
     * @return The table name
     */
    public String tableName() {
        return tblQName.name();
    }

    /**
     * Sets the table name
     *
     * @param tblName The table name.
     */
    public void tableName(String tblName) {
        this.tblQName.name(tblName);
    }

    /**
     * Returns the local file name.
     *
     * @return The local file name.
     */
    public String localFileName() {
        return localFileName;

    }

    /**
     * Sets the local file name.
     *
     * @param localFileName The local file name.
     */
    public void localFileName(String localFileName) {
        this.localFileName = localFileName;
    }

    /**
     * Returns the list of columns.
     *
     * @return The list of columns.
     */
    public List<String> columns() {
        return columns;
    }

    /**
     * Returns the input file format.
     *
     * @return The input file format.
     */
    public BulkLoadFormat inputFormat() {
        return inputFormat;
    }

    /**
     * Returns the batch size.
     *
     * @return The batch size.
     */
    public Integer batchSize() {
        return batchSize;
    }

    /**
     * Sets the batch size.
     *
     * @param batchSize The batch size.
     */
    public void batchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
