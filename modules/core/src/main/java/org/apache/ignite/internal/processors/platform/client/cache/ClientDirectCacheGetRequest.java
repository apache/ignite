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

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientObjectResponse;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.direct.ClientDirectWriteRequest;
import org.apache.ignite.internal.processors.platform.client.direct.ClientListenerDirectResponse;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.Scope;

/**
 * Cache get request.
 */
public class ClientDirectCacheGetRequest extends ClientCacheKeyRequest implements ClientDirectWriteRequest {
    /** */
    public static OperationContextAttribute<BinaryWriterEx> DIRECT_WRITER = OperationContextAttribute.newInstance();

    /** */
    public static GridCacheEntryInfo NOT_FOUND = new GridCacheEntryInfo();

    /** */
    public static GridCacheEntryInfo FOUND = new GridCacheEntryInfo();

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientDirectCacheGetRequest(BinaryReaderEx reader) {
        super(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx, BinaryWriterEx writer) {
        // TODO: disable fast path in case repair read enabled.
        // TODO: check when must disable fast path.
        try (Scope ignored = OperationContext.set(DIRECT_WRITER, writer)) {
            ClientResponse.encodeHeader(ctx, writer, requestId(), ClientStatus.SUCCESS, null, ctx.checkAffinityTopologyVersion());

            Object val = cache(ctx).get(key());

            if (val == FOUND)
                return new ClientListenerDirectResponse(requestId(), writer.out());

            return new ClientObjectResponse(requestId(), val);
        }
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process0(ClientConnectionContext ctx) {
        throw new UnsupportedOperationException("Unsupported!");
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<ClientResponse> processAsync0(ClientConnectionContext ctx) {
        throw new UnsupportedOperationException("Unsupported!");
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<ClientResponse> processAsync(ClientConnectionContext ctx, BinaryWriterEx writer) {
        throw new UnsupportedOperationException("Unsupported!");
    }
}
