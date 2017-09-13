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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * Scan query request.
 */
@SuppressWarnings("unchecked")
public class ClientCacheScanQueryRequest extends ClientCacheRequest {
    /** Java filter. */
    private static final byte FILTER_PLATFORM_JAVA = 1;

    /** .NET filter. */
    private static final byte FILTER_PLATFORM_DOTNET = 2;

    /** C++ filter. */
    private static final byte FILTER_PLATFORM_CPP = 3;

    /** Local flag. */
    private final boolean local;

    /** Page size. */
    private final int pageSize;

    /** Partition. */
    private final Integer partition;

    /** Filter platform. */
    private final byte filterPlatform;

    /** Filter object. */
    private final Object filterObject;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    public ClientCacheScanQueryRequest(BinaryRawReaderEx reader) {
        super(reader);

        filterPlatform = reader.readByte();

        switch (filterPlatform) {
            case GridBinaryMarshaller.NULL:
                filterObject = null;
                break;

            case FILTER_PLATFORM_JAVA:
            case FILTER_PLATFORM_DOTNET:
            case FILTER_PLATFORM_CPP:
                filterObject = reader.readObjectDetached();
                break;

            default:
                throw new UnsupportedOperationException("Invalid client ScanQuery filter code: " + filterPlatform);
        }

        pageSize = reader.readInt();
        partition = reader.readBoolean() ? reader.readInt() : null;
        local = reader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        ScanQuery qry = new ScanQuery()
            .setLocal(local)
            .setPageSize(pageSize)
            .setPartition(partition)
            .setFilter(createFilter(ctx));

        QueryCursor cur = cacheWithBinaryFlag(ctx).query(qry);

        ClientCacheScanQueryCursor cliCur = new ClientCacheScanQueryCursor((QueryCursorEx) cur, pageSize);

        long cursorId = ctx.resources().put(cliCur);

        return new ClientCacheScanQueryResponse(requestId(), cursorId, cliCur);
    }

    /**
     * Creates the filter.
     *
     * @return Filter.
     * @param ctx Context.
     */
    private IgniteBiPredicate createFilter(ClientConnectionContext ctx) {
        if (filterObject == null) {
            return null;
        }

        switch (filterPlatform) {
            case FILTER_PLATFORM_JAVA:
                return ((BinaryObject) filterObject).deserialize();

            case FILTER_PLATFORM_DOTNET:
                PlatformContext platformCtx = ctx.kernalContext().platform().context();

                String curPlatform = platformCtx.platform();

                if (!PlatformUtils.PLATFORM_DOTNET.equals(curPlatform)) {
                    throw new IgniteException("ScanQuery filter platform is " + PlatformUtils.PLATFORM_DOTNET +
                        ", current platform is " + curPlatform);
                }

                return platformCtx.createCacheEntryFilter(filterObject, 0);

            default:
                throw new UnsupportedOperationException("Invalid client ScanQuery filter code: " + filterPlatform);
        }
    }
}
