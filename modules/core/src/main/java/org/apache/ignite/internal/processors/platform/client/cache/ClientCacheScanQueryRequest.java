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
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxAwareRequest;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * Scan query request.
 */
@SuppressWarnings("unchecked")
public class ClientCacheScanQueryRequest extends ClientCacheDataRequest implements ClientTxAwareRequest {
    /** Java filter. */
    private static final byte FILTER_PLATFORM_JAVA = 1;

    /** .NET filter. */
    private static final byte FILTER_PLATFORM_DOTNET = 2;

    /** C++ filter. */
    private static final byte FILTER_PLATFORM_CPP = 3;

    /** Local flag. */
    private final boolean loc;

    /** Page size. */
    private final int pageSize;

    /** Partition. */
    private final Integer part;

    /** Filter platform. */
    private final byte filterPlatform;

    /** Filter object. */
    private final Object filterObj;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    public ClientCacheScanQueryRequest(BinaryRawReaderEx reader) {
        super(reader);

        filterObj = reader.readObjectDetached();

        filterPlatform = filterObj == null ? 0 : reader.readByte();

        pageSize = reader.readInt();

        int part0 = reader.readInt();
        part = part0 < 0 ? null : part0;

        loc = reader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteCache cache = filterPlatform == FILTER_PLATFORM_JAVA && !isKeepBinary() ? rawCache(ctx) : cache(ctx);

        ScanQuery qry = new ScanQuery()
            .setLocal(loc)
            .setPageSize(pageSize)
            .setPartition(part)
            .setFilter(createFilter(ctx));

        ctx.incrementCursors();

        try {
            QueryCursor cur = cache.query(qry);

            ClientCacheEntryQueryCursor cliCur = new ClientCacheEntryQueryCursor(cur, pageSize, ctx);

            long cursorId = ctx.resources().put(cliCur);

            cliCur.id(cursorId);

            return new ClientCacheQueryResponse(requestId(), cliCur);
        }
        catch (Exception e) {
            ctx.decrementCursors();

            throw e;
        }
    }

    /**
     * Creates the filter.
     *
     * @return Filter.
     * @param ctx Context.
     */
    private IgniteBiPredicate createFilter(ClientConnectionContext ctx) {
        if (filterObj == null)
            return null;

        switch (filterPlatform) {
            case FILTER_PLATFORM_JAVA:
                return ((BinaryObject)filterObj).deserialize();

            case FILTER_PLATFORM_DOTNET:
                PlatformContext platformCtx = ctx.kernalContext().platform().context();

                String curPlatform = platformCtx.platform();

                if (!PlatformUtils.PLATFORM_DOTNET.equals(curPlatform)) {
                    throw new IgniteException("ScanQuery filter platform is " + PlatformUtils.PLATFORM_DOTNET +
                        ", current platform is " + curPlatform);
                }

                return platformCtx.createCacheEntryFilter(filterObj, 0);

            case FILTER_PLATFORM_CPP:
            default:
                throw new UnsupportedOperationException("Invalid client ScanQuery filter code: " + filterPlatform);
        }
    }
}
