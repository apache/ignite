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

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetProxy;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Ignite set get or update request.
 */
public class ClientIgniteSetGetOrCreateRequest extends ClientRequest {
    /** Name. */
    private final String name;

    /** Config. */
    private final CollectionConfiguration collectionConfiguration;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientIgniteSetGetOrCreateRequest(BinaryRawReader reader) {
        super(reader);

        name = reader.readString();
        boolean create = reader.readBoolean();

        collectionConfiguration = create
                ? new CollectionConfiguration()
                .setAtomicityMode(CacheAtomicityMode.fromOrdinal(reader.readByte()))
                .setCacheMode(CacheMode.fromOrdinal(reader.readByte()))
                .setBackups(reader.readInt())
                .setGroupName(reader.readString())
                .setOffHeapMaxMemory(reader.readLong())
                .setCollocated(reader.readBoolean())
                : null;
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        GridCacheSetProxy<Object> set = (GridCacheSetProxy<Object>) ctx
                .kernalContext()
                .grid()
                .set(name, collectionConfiguration);

        if (set == null)
            return new Response(requestId(), null, false);

        return new Response(requestId(), set.delegate().id(), set.collocated());
    }

    /**
     * Response.
     */
    private static class Response extends ClientResponse {
        /** */
        private final IgniteUuid id;

        /** */
        private final boolean collocated;

        /**
         * Constructor.
         *
         * @param reqId Request id.
         * @param id Set id.
         */
        public Response(long reqId, IgniteUuid id, boolean collocated) {
            super(reqId);

            this.id = id;
            this.collocated = collocated;
        }

        /** {@inheritDoc} */
        @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
            super.encode(ctx, writer);

            if (id != null) {
                writer.writeBoolean(true);

                writer.writeLong(id.globalId().getMostSignificantBits());
                writer.writeLong(id.globalId().getLeastSignificantBits());
                writer.writeLong(id.localId());
                writer.writeBoolean(collocated);
            } else
                writer.writeBoolean(false);
        }
    }
}
