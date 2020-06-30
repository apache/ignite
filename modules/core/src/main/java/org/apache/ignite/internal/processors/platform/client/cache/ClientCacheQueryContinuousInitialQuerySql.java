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

import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcStatementType;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Initial query holder.
 */
@SuppressWarnings({"rawtypes"})
class ClientCacheQueryContinuousInitialQuerySql extends ClientCacheQueryContinuousInitialQuery {
    /** */
    private final String schema;

    /** */
    private final String sql;

    /** */
    private final Object[] args;

    /** */
    private final int pageSize;

    /** */
    private final JdbcStatementType stmtType;

    /** */
    private final boolean distributedJoins;

    /** */
    private final boolean loc;

    /** */
    private final boolean enforceJoinOrder;

    /** */
    private final boolean collocated;

    /** */
    private final boolean lazy;

    /** */
    private final int timeout;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    public ClientCacheQueryContinuousInitialQuerySql(BinaryRawReaderEx reader) {

        schema = reader.readString();
        pageSize = reader.readInt();
        sql = reader.readString();
        args = PlatformCache.readQueryArgs(reader);
        stmtType = JdbcStatementType.fromOrdinal(reader.readByte());
        distributedJoins = reader.readBoolean();
        loc = reader.readBoolean();
        enforceJoinOrder = reader.readBoolean();
        collocated = reader.readBoolean();
        lazy = reader.readBoolean();
        timeout = (int) reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public Query getQuery(GridKernalContext ctx) {
        SqlFieldsQuery qry = stmtType == JdbcStatementType.ANY_STATEMENT_TYPE
                ? new SqlFieldsQuery(sql)
                : new SqlFieldsQueryEx(sql,stmtType == JdbcStatementType.SELECT_STATEMENT_TYPE);

        qry.setSchema(schema)
                .setPageSize(pageSize)
                .setArgs(args)
                .setDistributedJoins(distributedJoins)
                .setLocal(loc)
                .setEnforceJoinOrder(enforceJoinOrder)
                .setCollocated(collocated)
                .setLazy(lazy);

        if (timeout >= 0)
            qry.setTimeout(timeout, TimeUnit.MILLISECONDS);

        return qry;
    }

    /** {@inheritDoc} */
    @Override public ClientCacheQueryCursor getClientCursor(QueryCursor cursor, ClientConnectionContext ctx) {
        return new ClientCacheFieldsQueryCursor((QueryCursorEx<List<?>>) cursor, pageSize, ctx, false);
    }
}
