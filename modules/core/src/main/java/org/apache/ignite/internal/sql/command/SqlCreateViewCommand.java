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
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.sql.SqlKeyword.AS;
import static org.apache.ignite.internal.sql.SqlKeyword.SELECT;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;

/**
 * CREATE VIEW command.
 */
public class SqlCreateViewCommand implements SqlCommand {
    /** Schema name. */
    private String schemaName;

    /** View name. */
    private String viewName;

    /** View SQL. */
    private String viewSql;

    /** REPLACE flag. */
    private boolean replace;

    /**
     * Default constructor.
     */
    public SqlCreateViewCommand() {
        // No-op.
    }

    /**
     * @param schemaName Schema name.
     * @param viewName View name.
     * @param viewSql View SQL.
     * @param replace Replace flag.
     */
    public SqlCreateViewCommand(String schemaName, String viewName, String viewSql, boolean replace) {
        this.schemaName = schemaName;
        this.viewName = viewName;
        this.viewSql = viewSql;
        this.replace = replace;
    }

    /**
     * @return View name.
     */
    public String viewName() {
        return viewName;
    }

    /**
     * @return View SQL.
     */
    public String viewSql() {
        return viewSql;
    }

    /**
     * @return REPLACE flag.
     */
    public boolean replace() {
        return replace;
    }

    /**
     * Sets REPLACE flag.
     */
    public SqlCreateViewCommand replace(boolean replace) {
        this.replace = replace;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        SqlQualifiedName viewQName = parseQualifiedIdentifier(lex);

        schemaName = viewQName.schemaName();
        viewName = viewQName.name();

        skipIfMatchesKeyword(lex, AS);

        skipIfMatchesKeyword(lex, SELECT);

        int viewSqlPos = lex.tokenPosition();

        while (lex.shift() && lex.lookAhead().tokenType() != SqlLexerTokenType.SEMICOLON) /* No-op. */;

        viewSql = lex.eod() ? lex.sql().substring(viewSqlPos) : lex.sql().substring(viewSqlPos, lex.position());

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlCreateViewCommand.class, this);
    }
}
