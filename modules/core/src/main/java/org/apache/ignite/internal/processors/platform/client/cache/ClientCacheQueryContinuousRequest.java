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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformJavaObjectFactoryProxy;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientPlatform;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;

import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEventFilter;

/**
 * Continuous query request.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientCacheQueryContinuousRequest extends ClientCacheRequest {

    /** Query. */
    private final ContinuousQuery qry;

    /** */
    private final Object filter;

    /** */
    private final byte filterPlatform;

    /** */
    private final ClientCacheQueryContinuousInitialQuery initQry;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    public ClientCacheQueryContinuousRequest(BinaryRawReaderEx reader) {
        super(reader);

        int pageSize = reader.readInt();
        long timeInterval = reader.readLong();
        boolean includeExpired = reader.readBoolean();

        filter = reader.readObjectDetached();
        filterPlatform = filter == null ? 0 : reader.readByte();
        initQry = ClientCacheQueryContinuousInitialQuery.read(reader);

        qry = new ContinuousQuery();

        qry.setPageSize(pageSize)
           .setTimeInterval(timeInterval)
           .setIncludeExpired(includeExpired);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        qry.setRemoteFilterFactory(getFilterFactory(ctx));

        ctx.incrementCursors();

        try {
            IgniteCache cache = filterPlatform == ClientPlatform.JAVA && !isKeepBinary() ? rawCache(ctx) : cache(ctx);

            ClientCacheQueryContinuousHandle handle = new ClientCacheQueryContinuousHandle(ctx);
            qry.setLocalListener(handle);

            if (initQry != null)
                qry.setInitialQuery(initQry.getQuery(ctx.kernalContext()));

            QueryCursor cursor = cache.query(qry);
            long cursorId = ctx.resources().put(cursor);

            return new ClientCacheQueryContinuousResponse(requestId(), handle, cursorId);
        }
        catch (Exception e) {
            ctx.decrementCursors();
            throw e;
        }
    }

    /**
     * Gets the filter factory.
     *
     * @param ctx Connection context.
     * @return Filter factory or null.
     */
    private Factory<? extends CacheEntryEventFilter> getFilterFactory(ClientConnectionContext ctx) {
        if (filter == null)
            return null;

        if (!(filter instanceof BinaryObject))
            throw new IgniteClientException(ClientStatus.FAILED,
                    "Filter must be a BinaryObject: " + filter.getClass());

        BinaryObjectImpl bo = (BinaryObjectImpl) filter;

        switch (filterPlatform) {
            case ClientPlatform.JAVA:
                return bo.deserialize();

            case ClientPlatform.DOTNET: {
                if (bo.typeId() == GridBinaryMarshaller.PLATFORM_JAVA_OBJECT_FACTORY_PROXY) {
                    PlatformJavaObjectFactoryProxy prx = bo.deserialize();

                    CacheEntryEventSerializableFilter rmtFilter =
                            (CacheEntryEventSerializableFilter) prx.factory(ctx.kernalContext()).create();

                    return FactoryBuilder.factoryOf(rmtFilter);
                }

                PlatformContext platformCtx = ctx.kernalContext().platform().context();

                String curPlatform = platformCtx.platform();

                if (!PlatformUtils.PLATFORM_DOTNET.equals(curPlatform))
                    throw new IgniteClientException(ClientStatus.FAILED, "ScanQuery filter platform is " +
                            PlatformUtils.PLATFORM_DOTNET + ", current platform is " + curPlatform);

                return FactoryBuilder.factoryOf(platformCtx.createContinuousQueryFilter(filter));
            }
            default:
                throw new IgniteClientException(ClientStatus.FAILED, "Unsupported filter platform: " + filterPlatform);
        }
    }
}
