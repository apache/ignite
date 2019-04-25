/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
 * DROP INDEX command.
 */
public class SqlDropIndexCommand implements SqlCommand {
    /** Schema name. */
    private String schemaName;

    /** Index name. */
    private String idxName;

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
     * @return Index name.
     */
    public String indexName() {
        return idxName;
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
        idxName = idxQName.name();

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlDropIndexCommand.class, this);
    }
}
