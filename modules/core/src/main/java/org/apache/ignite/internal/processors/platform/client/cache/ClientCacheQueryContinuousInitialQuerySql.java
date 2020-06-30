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

import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
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
    private final SqlFieldsQuery qry;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    public ClientCacheQueryContinuousInitialQuerySql(BinaryRawReaderEx reader) {
        String schema = reader.readString();
        int pageSize = reader.readInt();
        String sql = reader.readString();
        Object[] args = PlatformCache.readQueryArgs(reader);
        boolean distributedJoins = reader.readBoolean();
        boolean loc = reader.readBoolean();
        boolean enforceJoinOrder = reader.readBoolean();
        boolean collocated = reader.readBoolean();
        boolean lazy = reader.readBoolean();
        long timeout = reader.readLong();

        qry = new SqlFieldsQuery(sql)
                .setSchema(schema)
                .setPageSize(pageSize)
                .setArgs(args)
                .setDistributedJoins(distributedJoins)
                .setLocal(loc)
                .setEnforceJoinOrder(enforceJoinOrder)
                .setCollocated(collocated)
                .setLazy(lazy);

        if (timeout >= 0)
            qry.setTimeout((int) timeout, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public Query getQuery(GridKernalContext ctx) {
        return qry;
    }

    /** {@inheritDoc} */
    @Override public ClientCacheQueryCursor getClientCursor(QueryCursor cursor, ClientConnectionContext ctx) {
        return new ClientCacheFieldsQueryCursor((QueryCursorEx<List<?>>) cursor, qry.getPageSize(), ctx, false);
    }
}
