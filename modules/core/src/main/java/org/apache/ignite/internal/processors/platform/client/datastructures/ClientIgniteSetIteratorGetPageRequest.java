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

import java.util.Iterator;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import static org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetIteratorStartRequest.writePage;

/**
 * Ignite set iterator next page request.
 */
@SuppressWarnings("rawtypes")
public class ClientIgniteSetIteratorGetPageRequest extends ClientRequest {
    /** Page size. */
    private final int pageSize;

    /** Resource id. */
    private final long resId;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientIgniteSetIteratorGetPageRequest(BinaryRawReader reader) {
        super(reader);

        resId = reader.readLong();
        pageSize = reader.readInt();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        return new Response(requestId(), ctx.resources().get(resId));
    }

    /**
     * Response.
     */
    private class Response extends ClientResponse {
        /** Iterator. */
        private final Iterator iter;

        /**
         * Constructor.
         *
         * @param reqId Request id.
         * @param iter Iterator.
         */
        public Response(long reqId, Iterator iter) {
            super(reqId);

            this.iter = iter;
        }

        /** {@inheritDoc} */
        @Override public void encode(ClientConnectionContext ctx, BinaryWriterEx writer) {
            super.encode(ctx, writer);

            writePage(writer, iter, pageSize);

            if (!iter.hasNext())
                ctx.resources().release(resId);
        }
    }
}
