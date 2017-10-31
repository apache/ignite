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
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.sql.SqlKeyword.CASCADE;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlKeyword.RESTRICT;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnsupportedIfMatchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIfExists;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;

/**
 * Drop table command.
 */
public class SqlDropTableCommand implements SqlCommand {
    /** Schema name. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** IF EXISTS flag. */
    private boolean ifExists;

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
     * @return IF EXISTS flag.
     */
    public boolean ifExists() {
        return ifExists;
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        ifExists = parseIfExists(lex);

        SqlQualifiedName idxQName = parseQualifiedIdentifier(lex, IF);

        schemaName = idxQName.schemaName();
        tblName = idxQName.name();

        parseRestrictCascade(lex);

        return this;
    }

    /**
     * @param lex Lexer.
     */
    private static void parseRestrictCascade(SqlLexer lex) {
        errorUnsupportedIfMatchesKeyword(lex.lookAhead(), CASCADE, RESTRICT);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlDropTableCommand.class, this);
    }
}
