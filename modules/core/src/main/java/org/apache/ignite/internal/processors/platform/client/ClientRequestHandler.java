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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.SqlListenerResponse;

/**
 * Thin client request handler.
 */
public class ClientRequestHandler implements SqlListenerRequestHandler {
    /** */
    private static final short OP_CACHE_GET = 1;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /** Cache context. */
    private final GridCacheSharedContext cacheSharedCtx;

    /**
     * Ctor.
     *
     * @param ctx Kernal context.
     */
    public ClientRequestHandler(GridKernalContext ctx) {
        assert ctx != null;

        this.ctx = ctx;

        CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();
        marsh = cacheObjProc.marshaller();

        cacheSharedCtx = ctx.cache().context();
    }

    /** {@inheritDoc} */
    @Override public SqlListenerResponse handle(SqlListenerRequest req) {
        ClientRequest req0 = (ClientRequest)req;

        BinaryInputStream inStream = new BinaryHeapInputStream(req0.getData());
        BinaryRawReaderEx reader = marsh.reader(inStream);

        int requestId = reader.readInt();

        BinaryHeapOutputStream outStream = new BinaryHeapOutputStream(32);
        BinaryRawWriter writer = marsh.writer(outStream);

        writer.writeInt(requestId);

        processCommand(reader, writer);

        return new ClientResponse(SqlListenerResponse.STATUS_SUCCESS, null, outStream.array());
    }

    /**
     * Processes the command.
     *
     * @param reader Reader.
     * @param writer Writer.
     */
    @SuppressWarnings("unchecked")
    private void processCommand(BinaryRawReaderEx reader, BinaryRawWriter writer) {
        reader.readByte();  //  Flags: Compression, etc.
        writer.writeBoolean(true);  // Success (may include additional flags)

        short opCode = reader.readShort();

        switch (opCode) {
            case OP_CACHE_GET: {
                IgniteCache cache = getIgniteCache(reader);

                Object key = reader.readObjectDetached();
                Object val = cache.get(key);

                writer.writeObject(val);
                return;
            }
        }

        throw new IgniteException("Invalid operation: " + opCode);
    }

    /**
     * Gets the cache.
     *
     * @param reader Reader
     * @return Cache.
     */
    private IgniteCache getIgniteCache(BinaryRawReader reader) {
        int cacheId = reader.readInt();
        reader.readByte();  // Flags: withSkipStore, etc
        String cacheName = cacheSharedCtx.cacheContext(cacheId).cache().name();
        return ctx.grid().cache(cacheName).withKeepBinary();
    }

    /** {@inheritDoc} */
    @Override public SqlListenerResponse handleException(Exception e) {
        return null;
    }
}