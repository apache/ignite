/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerToken;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.sql.SqlKeyword.LOGGING;
import static org.apache.ignite.internal.sql.SqlKeyword.NOLOGGING;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;

/**
 * ALTER TABLE command.
 */
public class SqlAlterTableCommand implements SqlCommand  {
    /** Schema name. */
    private String schemaName;

    /** Schema name. */
    private String tblName;

    /** Logging flag. */
    private Boolean logging;

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
     * @return Logging status or {@code null} if no changes to logging is requested.
     */
    @Nullable public Boolean logging() {
        return logging;
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        SqlQualifiedName tblQName = parseQualifiedIdentifier(lex);

        schemaName = tblQName.schemaName();
        tblName = tblQName.name();

        parseLogging(lex);

        if (!hasCommands())
            throw errorUnexpectedToken(lex, LOGGING, NOLOGGING);

        return this;
    }

    /**
     * Parse LOGGING and/or NOLOGGING statement.
     */
    private void parseLogging(SqlLexer lex) {
        SqlLexerToken token = lex.lookAhead();

        if (matchesKeyword(token, LOGGING)) {
            lex.shift();

            logging = true;
        }
        else if (matchesKeyword(token, NOLOGGING)) {
            lex.shift();

            logging = false;
        }
    }

    /**
     * Check if statement contain any commands.
     *
     * @return {@code True} if statement is not dummy and contains at least one command.
     */
    private boolean hasCommands() {
        return logging != null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlAlterTableCommand.class, this);
    }
}
