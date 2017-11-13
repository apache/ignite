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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcStatementType;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Sql query request.
 */
@SuppressWarnings("unchecked")
public class ClientCacheSqlFieldsQueryRequest extends ClientCacheRequest {
    /** Query. */
    private final SqlFieldsQuery qry;

    /** Include field names flag. */
    private final boolean includeFieldNames;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    public ClientCacheSqlFieldsQueryRequest(BinaryRawReaderEx reader) {
        super(reader);

        // Same request format as in JdbcQueryExecuteRequest.
        SqlFieldsQuery qry = new SqlFieldsQuery("")
                .setSchema(reader.readString())
                .setPageSize(reader.readInt());

        reader.readInt();  // maxRows

        qry.setSql(reader.readString())
                .setArgs(PlatformCache.readQueryArgs(reader));

        JdbcStatementType stmtType = JdbcStatementType.fromOrdinal(reader.readByte());

        qry.setDistributedJoins(reader.readBoolean())
                .setLocal(reader.readBoolean())
                .setReplicatedOnly(reader.readBoolean())
                .setEnforceJoinOrder(reader.readBoolean())
                .setCollocated(reader.readBoolean())
                .setLazy(reader.readBoolean())
                .setTimeout((int) reader.readLong(), TimeUnit.MILLISECONDS);

        includeFieldNames = reader.readBoolean();

        if (stmtType != JdbcStatementType.ANY_STATEMENT_TYPE) {
            qry = new SqlFieldsQueryEx(qry.getSql(),
                    stmtType == JdbcStatementType.SELECT_STATEMENT_TYPE)
                    .setSchema(qry.getSchema())
                    .setPageSize(qry.getPageSize())
                    .setArgs(qry.getArgs())
                    .setDistributedJoins(qry.isDistributedJoins())
                    .setLocal(qry.isLocal())
                    .setReplicatedOnly(qry.isReplicatedOnly())
                    .setEnforceJoinOrder(qry.isEnforceJoinOrder())
                    .setCollocated(qry.isCollocated())
                    .setLazy(qry.isLazy())
                    .setTimeout(qry.getTimeout(), TimeUnit.MILLISECONDS);
        }

        this.qry = qry;
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteCache cache = cache(ctx);

        ctx.incrementCursors();

        try {
            FieldsQueryCursor<List> cur = cache.query(qry);

            ClientCacheFieldsQueryCursor cliCur = new ClientCacheFieldsQueryCursor(
                    cur, qry.getPageSize(), ctx);

            long cursorId = ctx.resources().put(cliCur);

            cliCur.id(cursorId);

            return new ClientCacheSqlFieldsQueryResponse(requestId(), cliCur, cur, includeFieldNames);
        }
        catch (Exception e) {
            ctx.decrementCursors();

            throw e;
        }
    }
}
