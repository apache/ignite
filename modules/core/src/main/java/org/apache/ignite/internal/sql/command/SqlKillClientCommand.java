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

import org.apache.ignite.internal.management.kill.ClientConnectionDropTask;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;

/**
 * KILL CLIENT command.
 *
 * @see ClientConnectionDropTask
 * @see ClientConnectionView#connectionId()
 */
public class SqlKillClientCommand implements SqlCommand {
    /** Connections id. */
    private Long connectionId;

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        if (lex.shift()) {
            if (lex.tokenType() == SqlLexerTokenType.DEFAULT) {
                String connIdStr = lex.token();

                if (!"ALL".equals(connIdStr))
                    connectionId = Long.parseLong(connIdStr);

                return this;
            }
        }

        throw SqlParserUtils.error(lex, "Expected client connection id or ALL.");
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }

    /** @return Connection id to drop. */
    public Long connectionId() {
        return connectionId;
    }
}
