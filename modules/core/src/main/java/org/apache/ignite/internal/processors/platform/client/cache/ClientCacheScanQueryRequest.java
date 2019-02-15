/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
