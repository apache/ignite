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

package org.apache.ignite.internal.processors.platform.client.datastructures;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetProxy;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Ignite set get or update request.
 */
public class ClientIgniteSetCreateRequest extends ClientIgniteSetRequest {
    /** Cache atomicity mode. */
    private final CollectionConfiguration collectionConfiguration;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientIgniteSetCreateRequest(BinaryRawReader reader) {
        super(reader);

        collectionConfiguration = new CollectionConfiguration()
                .setAtomicityMode(CacheAtomicityMode.fromOrdinal(reader.readByte()))
                .setCacheMode(CacheMode.fromOrdinal(reader.readByte()))
                .setBackups(reader.readInt())
                .setOffHeapMaxMemory(reader.readLong())
                .setCollocated(reader.readBoolean());
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        try {
            GridCacheSetProxy<Object> set = (GridCacheSetProxy<Object>)ctx.kernalContext().dataStructures().set(
                    name(), groupName(), collectionConfiguration);

            IgniteUuid id = set.delegate().id();

            return new Response(requestId(), id);
        } catch (IgniteCheckedException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /**
     * Response.
     */
    private static class Response extends ClientResponse {
        /** */
        private final IgniteUuid id;

        /**
         * Constructor.
         *
         * @param reqId Request id.
         * @param id Set id.
         */
        public Response(long reqId, IgniteUuid id) {
            super(reqId);

            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
            super.encode(ctx, writer);

            writer.writeUuid(id.globalId());
            writer.writeLong(id.localId());
        }
    }
}
