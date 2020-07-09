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

import java.util.UUID;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.spi.systemview.view.ScanQueryView;

/**
 * KILL SCAN_QUERY command.
 *
 * @see QueryMXBean#cancelScan(String, String, Long)
 * @see ScanQueryView#originNodeId()
 * @see ScanQueryView#cacheName()
 * @see ScanQueryView#queryId()
 */
public class SqlKillScanQueryCommand implements SqlCommand {
    /** KILL SCAN format message. */
    public static final String KILL_SCAN_QRY_FORMAT =
        "Format of the query is KILL SCAN '6fa749ee-7cf8-4635-be10-36a1c75267a7_54321' 'cache-name' 1";

    /** Origin node id. */
    private UUID originNodeId;

    /** Cache name. */
    private String cacheName;

    /** Query id. */
    private long qryId;

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        if (lex.shift()) {
            if (lex.tokenType() == SqlLexerTokenType.STRING) {
                originNodeId = UUID.fromString(lex.token());

                if (lex.shift() && lex.tokenType() == SqlLexerTokenType.STRING) {
                    cacheName = lex.token();

                    if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT)
                        qryId = Long.parseLong(lex.token());
                    else
                        throw SqlParserUtils.error(lex, "Expected query id. " + KILL_SCAN_QRY_FORMAT);
                }
                else
                    throw SqlParserUtils.error(lex, "Expected cache name. " + KILL_SCAN_QRY_FORMAT);

                return this;
            }
        }

        throw SqlParserUtils.error(lex, "Expected origin node id. " + KILL_SCAN_QRY_FORMAT);
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }

    /** @return Origin node id. */
    public UUID getOriginNodeId() {
        return originNodeId;
    }

    /** @return Cache name. */
    public String getCacheName() {
        return cacheName;
    }

    /** @return Query id. */
    public long getQryId() {
        return qryId;
    }
}
