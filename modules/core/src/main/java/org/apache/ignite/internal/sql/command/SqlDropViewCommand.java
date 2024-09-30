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

import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIfExists;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;

/**
 * DROP VIEW command.
 */
public class SqlDropViewCommand implements SqlCommand {
    /** Schema name. */
    private String schemaName;

    /** View name. */
    private String viewName;

    /** IF EXISTS flag. */
    private boolean ifExists;

    /**
     * Default constructor.
     */
    public SqlDropViewCommand() {
        // No-op.
    }

    /**
     * @param schemaName Schema name.
     * @param viewName VIew name.
     * @param ifExists If exists clause.
     */
    public SqlDropViewCommand(String schemaName, String viewName, boolean ifExists) {
        this.schemaName = schemaName;
        this.viewName = viewName;
        this.ifExists = ifExists;
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
     * @return Index name.
     */
    public String viewName() {
        return viewName;
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

        SqlQualifiedName viewQName = ifExists ? parseQualifiedIdentifier(lex) : parseQualifiedIdentifier(lex, IF);

        schemaName = viewQName.schemaName();
        viewName = viewQName.name();

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlDropViewCommand.class, this);
    }
}
