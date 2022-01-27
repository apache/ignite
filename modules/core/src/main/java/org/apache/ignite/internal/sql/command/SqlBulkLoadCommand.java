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
import java.util.List;
import org.apache.ignite.internal.processors.bulkload.BulkLoadAckClientParameters;
import org.apache.ignite.internal.processors.bulkload.BulkLoadCsvFormat;
import org.apache.ignite.internal.processors.bulkload.BulkLoadFormat;
import org.apache.ignite.internal.sql.SqlKeyword;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseInt;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseString;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatches;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * A parser for a COPY command (called 'bulk load' in the code, since word 'copy' is too generic).
 */
public class SqlBulkLoadCommand implements SqlCommand {
    /** Local file name to send from client to server. */
    private String locFileName;

    /** Schema name + table name. */
    private SqlQualifiedName tblQName;

    /** User-specified list of columns. */
    private List<String> cols;

    /** File format. */
    private BulkLoadFormat inputFormat;

    /** Packet size (size of portion of a file sent in each sub-request). */
    private Integer packetSize;

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
        if (lex.lookAhead().tokenType() != SqlLexerTokenType.STRING)
            throw errorUnexpectedToken(lex.lookAhead(), "[file name: string]");

        lex.shift();

        locFileName = lex.token();
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

        cols = new ArrayList<>();

        do {
            cols.add(parseColumn(lex));
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
        return parseIdentifier(lex);
    }

    /**
     * Parses the format clause.
     *
     * @param lex The lexer.
     */
    private void parseFormat(SqlLexer lex) {
        skipIfMatchesKeyword(lex, SqlKeyword.FORMAT);

        String name = parseIdentifier(lex);

        switch (name.toUpperCase()) {
            case BulkLoadCsvFormat.NAME:
                BulkLoadCsvFormat fmt = new BulkLoadCsvFormat();

                // IGNITE-7537 will introduce user-defined values
                fmt.lineSeparator(BulkLoadCsvFormat.DEFAULT_LINE_SEPARATOR);
                fmt.fieldSeparator(BulkLoadCsvFormat.DEFAULT_FIELD_SEPARATOR);
                fmt.quoteChars(BulkLoadCsvFormat.DEFAULT_QUOTE_CHARS);
                fmt.commentChars(BulkLoadCsvFormat.DEFAULT_COMMENT_CHARS);
                fmt.escapeChars(BulkLoadCsvFormat.DEFAULT_ESCAPE_CHARS);

                parseCsvOptions(lex, fmt);

                inputFormat = fmt;

                break;

            default:
                throw error(lex, "Unknown format name: " + name +
                    ". Currently supported format is " + BulkLoadCsvFormat.NAME);
        }
    }

    /**
     * Parses CSV format options.
     *
     * @param lex The lexer.
     * @param format CSV format object to configure.
     */
    private void parseCsvOptions(SqlLexer lex, BulkLoadCsvFormat format) {
        while (lex.lookAhead().tokenType() == SqlLexerTokenType.DEFAULT) {
            switch (lex.lookAhead().token()) {
                case SqlKeyword.CHARSET: {
                    lex.shift();

                    String charsetName = parseString(lex);

                    format.inputCharsetName(charsetName);

                    break;
                }

                default:
                    return;
            }
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
                case SqlKeyword.PACKET_SIZE:
                    lex.shift();

                    int size = parseInt(lex);

                    if (!BulkLoadAckClientParameters.isValidPacketSize(size))
                        throw error(lex, BulkLoadAckClientParameters.packetSizeErrorMesssage(size));

                    packetSize = size;

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
    @Override public String schemaName() {
        return tblQName.schemaName();
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        tblQName.schemaName(schemaName);
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
        tblQName.name(tblName);
    }

    /**
     * Returns the local file name.
     *
     * @return The local file name.
     */
    public String localFileName() {
        return locFileName;
    }

    /**
     * Sets the local file name.
     *
     * @param locFileName The local file name.
     */
    public void localFileName(String locFileName) {
        this.locFileName = locFileName;
    }

    /**
     * Returns the list of columns.
     *
     * @return The list of columns.
     */
    public List<String> columns() {
        return cols;
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
     * Returns the packet size.
     *
     * @return The packet size.
     */
    public Integer packetSize() {
        return packetSize;
    }

    /**
     * Sets the packet size.
     *
     * @param packetSize The packet size.
     */
    public void packetSize(int packetSize) {
        this.packetSize = packetSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlBulkLoadCommand.class, this);
    }
}
