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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;

/**
 * Initial query holder.
 */
@SuppressWarnings("rawtypes")
abstract class ClientCacheQueryContinuousInitialQuery {
    /** */
    private static final byte TYPE_NONE = 0;

    /** */
    private static final byte TYPE_SCAN = 1;

    /** */
    private static final byte TYPE_SQL = 2;

    /**
     * Ctor.
     */
    protected ClientCacheQueryContinuousInitialQuery() {
        // No-op.
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
                return new ClientCacheQueryContinuousInitialQueryScan(reader);

            case TYPE_SQL:
                return new ClientCacheQueryContinuousInitialQuerySql(reader);

            default:
                throw new IgniteClientException(ClientStatus.FAILED, "Invalid initial query type: " + typ);
        }
    }

    /**
     * Gets the query.
     *
     * @return Query.
     */
    public abstract Query getQuery(GridKernalContext ctx);

    /**
     * Gets the client cursor.
     *
     * @param cursor Query cursor.
     * @param ctx Context.
     * @return Client cache query cursor according to query type.
     */
    public abstract ClientCacheQueryCursor getClientCursor(QueryCursor cursor, ClientConnectionContext ctx);
}
