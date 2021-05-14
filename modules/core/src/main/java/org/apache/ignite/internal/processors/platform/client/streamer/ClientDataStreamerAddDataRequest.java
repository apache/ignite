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
import org.apache.ignite.stream.StreamReceiver;

import java.util.Collection;

/**
 *
 */
public class ClientDataStreamerAddDataRequest extends ClientRequest {
    /**
     * Streamer flush flag mask.
     */
    private static final byte FLUSH_FLAG_MASK = 0x01;

    /**
     * Streamer end flag mask.
     */
    private static final byte END_FLAG_MASK = 0x02;

    /** */
    private final long streamerId;

    /** */
    private final byte flags;

    /**
     * Data entries.
     */
    private final Collection<DataStreamerEntry> entries;

    public ClientDataStreamerAddDataRequest(BinaryReaderExImpl reader) {
        super(reader);

        streamerId = reader.readLong();
        flags = reader.readByte();
        entries = ClientDataStreamerReader.read(reader);
    }

    /**
     * {@inheritDoc}
     */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        ClientDataStreamerHandle handle = ctx.resources().get(streamerId);
        IgniteDataStreamer<KeyCacheObject, CacheObject> dataStreamer = handle.getStreamer();

        // To remove data, pass null as a value for the key.
        dataStreamer.addData(entries);

        if ((flags & END_FLAG_MASK) != 0) {
            dataStreamer.close();
            ctx.resources().release(streamerId);
        } else if ((flags & FLUSH_FLAG_MASK) != 0)
            dataStreamer.flush();

        return new ClientResponse(requestId());
    }
}
