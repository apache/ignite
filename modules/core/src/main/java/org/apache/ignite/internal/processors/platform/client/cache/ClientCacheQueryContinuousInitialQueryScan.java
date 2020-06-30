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

import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcStatementType;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.jetbrains.annotations.Nullable;

/**
 * Initial query holder.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class ClientCacheQueryContinuousInitialQueryScan extends ClientCacheQueryContinuousInitialQuery {
   /** */
    @Nullable
    private final Object filter;

    /** */
    private final byte filterPlatform;

    /** */
    private final int pageSize;

    /** */
    private final Integer part;

    /** */
    private final boolean loc;

    /**
     * Ctor.
     *
     * @param filter Filter (for Scan query).
     * @param filterPlatform Filter platform.
     * @param pageSize Page size.
     * @param part Partition.
     * @param loc Local flag.
     */
    public ClientCacheQueryContinuousInitialQueryScan(
            @Nullable Object filter,
            byte filterPlatform,
            int pageSize,
            @Nullable Integer part,
            boolean loc) {
        this.filter = filter;
        this.filterPlatform = filterPlatform;
        this.pageSize = pageSize;
        this.part = part;
        this.loc = loc;
    }

    /**
     * Gets the query.
     *
     * @return Query.
     */
    @Override
    public Query getQuery(GridKernalContext ctx) {
        return new ScanQuery()
                .setFilter(ClientCacheScanQueryRequest.createFilter(ctx, filter, filterPlatform))
                .setPageSize(pageSize)
                .setLocal(loc)
                .setPartition(part);
    }

    /**
     * Gets the client cursor.
     *
     * @param cursor Query cursor.
     * @param ctx Context.
     * @return Client cache query cursor according to query type.
     */
    @Override
    public ClientCacheQueryCursor getClientCursor(QueryCursor cursor, ClientConnectionContext ctx) {
        return new ClientCacheEntryQueryCursor(cursor, pageSize, ctx, false);
    }
}
