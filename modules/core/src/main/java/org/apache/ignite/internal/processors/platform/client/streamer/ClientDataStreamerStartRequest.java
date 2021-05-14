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

package org.apache.ignite.internal.processors.platform.client.streamer;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientLongResponse;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRequest;
import org.apache.ignite.stream.StreamReceiver;

import java.util.Collection;

/**
 *
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientDataStreamerStartRequest extends ClientRequest {
    /** Allow overwrite flag mask. */
    private static final byte ALLOW_OVERWRITE_FLAG_MASK = 0x01;

    /** Skip store flag mask. */
    private static final byte SKIP_STORE_FLAG_MASK = 0x02;

    /** Keep binary flag mask. */
    private static final byte KEEP_BINARY_FLAG_MASK = 0x04;

    /** Streamer flush flag mask. */
    private static final byte FLUSH_FLAG_MASK = 0x08;

    /** Streamer close flag mask. */
    private static final byte CLOSE_FLAG_MASK = 0x10;

    /** */
    private final int cacheId;

    /** */
    private final byte flags;

    /** */
    private final int perNodeBufferSize;

    /** */
    private final int perThreadBufferSize;

    /** */
    private final StreamReceiver receiver;

    /** Data entries. */
    private final Collection<DataStreamerEntry> entries;

    /**
     * Ctor.
     *
     * @param reader Data reader.
     */
    public ClientDataStreamerStartRequest(BinaryReaderExImpl reader) {
        super(reader);

        cacheId = reader.readInt();
        flags = reader.readByte();
        perNodeBufferSize = reader.readInt();
        perThreadBufferSize = reader.readInt();
        receiver = (StreamReceiver) reader.readObject();
        entries = ClientDataStreamerReader.read(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        String cacheName = ClientCacheRequest.cacheDescriptor(ctx, cacheId).cacheName();
        IgniteDataStreamer<KeyCacheObject, CacheObject> dataStreamer = ctx.kernalContext().grid().dataStreamer(cacheName);

        if (perNodeBufferSize >= 0)
            dataStreamer.perNodeBufferSize(perNodeBufferSize);

        if (perThreadBufferSize >= 0)
            dataStreamer.perThreadBufferSize(perThreadBufferSize);

        dataStreamer.allowOverwrite((flags & ALLOW_OVERWRITE_FLAG_MASK) != 0);
        dataStreamer.skipStore((flags & SKIP_STORE_FLAG_MASK) != 0);
        dataStreamer.keepBinary((flags & KEEP_BINARY_FLAG_MASK) != 0);

        if (receiver != null)
            dataStreamer.receiver(receiver);

        dataStreamer.addData(entries);

        if ((flags & CLOSE_FLAG_MASK) != 0) {
            dataStreamer.close();

            return new ClientLongResponse(requestId(), 0);
        } else {
            if ((flags & FLUSH_FLAG_MASK) != 0)
                dataStreamer.flush();

            long rsrcId = ctx.resources().put(new ClientDataStreamerHandle(dataStreamer));

            return new ClientLongResponse(requestId(), rsrcId);
        }
    }
}
