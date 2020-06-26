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
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.jetbrains.annotations.Nullable;

/**
 * Initial query holder.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class ClientCacheQueryContinuousInitialQuery {
    /** */
    private static final byte TYPE_NONE = 0;

    /** */
    private static final byte TYPE_SCAN = 1;

    /** */
    private static final byte TYPE_SQL = 2;

    /** */
    private final byte type;

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

    /** */
    @Nullable
    private final String sql;

    /**
     * Ctor.
     *
     * @param type Query type.
     * @param filter Filter (for Scan query).
     * @param filterPlatform Filter platform.
     * @param pageSize Page size.
     * @param part Partition.
     * @param loc Local flag.
     * @param sql Sql.
     */
    public ClientCacheQueryContinuousInitialQuery(
            byte type,
            @Nullable Object filter,
            byte filterPlatform,
            int pageSize,
            @Nullable Integer part,
            boolean loc,
            @Nullable String sql) {
        assert type > TYPE_NONE && type <= TYPE_SQL;

        this.type = type;
        this.filter = filter;
        this.filterPlatform = filterPlatform;
        this.pageSize = pageSize;
        this.part = part;
        this.loc = loc;
        this.sql = sql;
    }

    /**
     * Reads the query.
     *
     * @param reader Reader.
     * @return Query or null.
     */
    public static ClientCacheQueryContinuousInitialQuery read(BinaryRawReaderEx reader) {
        byte typ = reader.readByte();

        switch (typ) {
            case TYPE_NONE:
                return null;

            case TYPE_SCAN:
                Object filter = reader.readObjectDetached();
                byte filterPlatform = filter == null ? 0 : reader.readByte();
                int pageSize = reader.readInt();

                int part0 = reader.readInt();
                Integer part = part0 < 0 ? null : part0;

                boolean loc = reader.readBoolean();

                return new ClientCacheQueryContinuousInitialQuery(typ, filter, filterPlatform, pageSize,
                        part, loc, null);

            case TYPE_SQL:
                // TODO
                return null;

            default:
                throw new IgniteClientException(ClientStatus.FAILED, "Invalid initial query type: " + typ);
        }
    }

    /**
     * Gets the query.
     *
     * @return Query.
     */
    public Query getQuery(GridKernalContext ctx) {
        switch (type) {
            case TYPE_SCAN:
                return new ScanQuery()
                        .setFilter(ClientCacheScanQueryRequest.createFilter(ctx, filter, filterPlatform))
                        .setPageSize(pageSize)
                        .setLocal(loc)
                        .setPartition(part);

            case TYPE_SQL:
                // TODO
                return new SqlFieldsQuery(sql);

            default:
                throw new IgniteException("Invalid initial query type: " + type);
        }
    }
}
