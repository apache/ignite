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
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.lang.IgniteUuid;

/**
 * KILL LOCK command.
 */
public class SqlKillLockCommand implements SqlCommand {
    /** Lock ID. */
    private IgniteUuid xid;

    /**
     * Default constructor.
     */
    public SqlKillLockCommand() {
        // No-op.
    }

    /**
     * @param xid Lock ID.
     */
    public SqlKillLockCommand(IgniteUuid xid) {
        this.xid = xid;
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        if (lex.shift()) {
            if (lex.tokenType() == SqlLexerTokenType.STRING) {
                try {
                    xid = IgniteUuid.fromString(lex.token());
                }
                catch (Exception e) {
                    throw SqlParserUtils.error(lex, "Failed to parse lock ID: " + e.getMessage());
                }

                return this;
            }
        }

        throw SqlParserUtils.error(lex, "Expected lock ID.");
    }

    /** @return Lock ID. */
    public IgniteUuid getXid() {
        return xid;
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }
}
