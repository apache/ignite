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
import org.apache.ignite.internal.sql.SqlKeyword;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/** FIXME SHQ */
public class SqlBulkLoadCommand implements SqlCommand {

    private String schemaName;
    private String tblName;
    private String localFileName;

    private BulkLoadFormat inputFormat;

    private List<String> columns;

    @Override public SqlCommand parse(SqlLexer lex) {
        // COPY is already parsed

        skipIfMatchesKeyword(lex, SqlKeyword.FROM);

        parseFileName(lex);

        parseTableName(lex);

        parseOptionalColumns(lex);

        parseFormat(lex);

        return this;
    }

    private void parseFileName(SqlLexer lex) {
        localFileName = parseIdentifier(lex);
    }

    private void parseTableName(SqlLexer lex) {
        skipIfMatchesKeyword(lex, SqlKeyword.INTO);

        SqlQualifiedName qname = parseQualifiedIdentifier(lex);

        schemaName = qname.schemaName();
        tblName = qname.name();
    }

    private void parseOptionalColumns(SqlLexer lex) {
        if (lex.lookAhead().tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT) {
            columns = null;
            return;
        }

        columns = new ArrayList<>();

        lex.shift();

        do {
            columns.add(parseColumn(lex));
        }
        while (!skipCommaOrRightParenthesis(lex));
    }

    private String parseColumn(SqlLexer lex) {
        String name = parseIdentifier(lex, SqlLexerTokenType.COMMA.asString(),
            SqlLexerTokenType.PARENTHESIS_RIGHT.asString());

        return name;
    }

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
     * Returns the schemaName.
     *
     * @return schemaName.
     */
    public String schemaName() {
        return schemaName;
    }

    @Override public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * Returns the tblName.
     *
     * @return tblName.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * Sets the tblName.
     *
     * @param tblName The tblName.
     */
    public void tblName(String tblName) {
        this.tblName = tblName;
    }

    public String localFileName() {
        return localFileName;

    }

    /**
     * Sets the localFileName.
     *
     * @param localFileName The localFileName.
     */
    public void localFileName(String localFileName) {
        this.localFileName = localFileName;
    }

    /**
     * Returns the columns.
     *
     * @return columns.
     */
    public List<String> columns() {
        return columns;
    }

    public BulkLoadFormat inputFormat() {
        return inputFormat;
    }
}
