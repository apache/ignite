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
 *
 */

package org.apache.ignite.internal.sql.command;

import java.util.UUID;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlParserUtils;

/**
 * KILL QUERY command.
 */
public class SqlKillQueryCommand implements SqlCommand {
    /** */
    private static final String ASYNC = "ASYNC";

    /** */
    private static final String EXPECTED_GLOBAL_QRY_ID_FORMAT = "Global query id should have format " +
        "'{node_id}_{query_id}', e.g. '6fa749ee-7cf8-4635-be10-36a1c75267a7_54321'";

    /** Node query id. */
    private long nodeQryId;

    /** Node id. */
    private UUID nodeId;

    /** Async flag. */
    private boolean async;

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        async = SqlParserUtils.skipIfMatchesOptionalKeyword(lex, ASYNC);

        parseGlobalQueryId(lex);

        return this;
    }

    /**
     * Parse global query id.
     *
     * @param lex Lexer.
     */
    private void parseGlobalQueryId(SqlLexer lex) {
        if (lex.shift()) {
            if (lex.tokenType() == SqlLexerTokenType.STRING) {
                String tok = lex.token();

                String[] ids = tok.split("_");

                if (ids.length == 2) {
                    try {
                        nodeId = UUID.fromString(ids[0]);

                        nodeQryId = Long.parseLong(ids[1]);

                        return;
                    }
                    catch (Exception ignore) {
                        // Fall through.
                    }

                   throw SqlParserUtils.error(lex, EXPECTED_GLOBAL_QRY_ID_FORMAT);
                }
            }
        }

        if (async)
            throw SqlParserUtils.error(lex, "Expected global query id. " + EXPECTED_GLOBAL_QRY_ID_FORMAT);
        else
            throw SqlParserUtils.error(lex, "Expected ASYNC token or global query id. "
                + EXPECTED_GLOBAL_QRY_ID_FORMAT);
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }

    /**
     * @return Node query id.
     */
    public long nodeQueryId() {
        return nodeQryId;
    }

    /**
     * @return Node order id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Async flag.
     */
    public boolean async() {
        return async;
    }
}
