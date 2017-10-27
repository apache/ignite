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
import org.apache.ignite.internal.sql.SqlParserToken;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.sql.SqlKeyword.EXISTS;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

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
     * @return IF EXISTS flag.
     */
    public boolean ifExists() {
        return ifExists;
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        parseIfExists(lex);

        SqlQualifiedName idxQName = parseQualifiedIdentifier(lex, IF);

        schemaName = idxQName.schemaName();
        tblName = idxQName.name();

        return this;
    }

    /**
     * Process IF EXISTS for DROP INDEX.
     *
     * @param lex Lexer.
     */
    private void parseIfExists(SqlLexer lex) {
        SqlParserToken token = lex.lookAhead();

        if (token != null && matchesKeyword(token, IF)) {
            lex.shift();

            skipIfMatchesKeyword(lex, EXISTS);

            ifExists = true;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlDropTableCommand.class, this);
    }
}
