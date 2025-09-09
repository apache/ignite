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
import org.apache.ignite.IgniteSet;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Ignite set iterator start request.
 */
@SuppressWarnings("rawtypes")
public class ClientIgniteSetIteratorStartRequest extends ClientIgniteSetRequest {
    /** Page size. */
    private final int pageSize;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientIgniteSetIteratorStartRequest(BinaryRawReader reader) {
        super(reader);

        pageSize = reader.readInt();
    }

    /** {@inheritDoc} */
    @Override protected ClientResponse process(IgniteSet<Object> set) {
        return new Response(requestId(), set.iterator());
    }

    /**
     * Writes next page to the writer.
     *
     * @param writer Writer.
     */
    static void writePage(BinaryWriterEx writer, Iterator iter, int pageSize) {
        int cntPos = writer.reserveInt();
        int cnt = 0;

        while (cnt < pageSize && iter.hasNext()) {
            writer.writeObject(iter.next());

            cnt++;
        }

        writer.writeInt(cntPos, cnt);
        writer.writeBoolean(iter.hasNext());
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

            if (iter.hasNext()) {
                long resId = ctx.resources().put(iter);
                writer.writeLong(resId);
            }
        }
    }
}
