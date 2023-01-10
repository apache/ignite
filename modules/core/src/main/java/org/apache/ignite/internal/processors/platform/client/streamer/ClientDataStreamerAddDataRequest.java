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

import java.util.Collection;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.CLOSE;
import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.FLUSH;
import static org.apache.ignite.internal.processors.platform.utils.PlatformUtils.ObjectWithBytes;

/**
 * Adds data to the existing streamer.
 */
public class ClientDataStreamerAddDataRequest extends ClientDataStreamerRequest {
    /** */
    private final long streamerId;

    /** */
    private final byte flags;

    /** */
    private final Collection<T2<ObjectWithBytes, ObjectWithBytes>> entries;

    /**
     * Constructor.
     *
     * @param reader Data reader.
     */
    public ClientDataStreamerAddDataRequest(BinaryReaderExImpl reader) {
        super(reader);

        streamerId = reader.readLong();
        flags = reader.readByte();
        entries = read(reader);
    }

    /**
     * {@inheritDoc}
     */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        ClientDataStreamerHandle handle = ctx.resources().get(streamerId);
        DataStreamerImpl<KeyCacheObject, CacheObject> dataStreamer =
            (DataStreamerImpl<KeyCacheObject, CacheObject>)handle.getStreamer();

        try {
            String cacheName = handle.getStreamer().cacheName();

            CacheObjectValueContext cotx = ctx.kernalContext().cache().cache(cacheName).context().cacheObjectContext();

            Collection<DataStreamerEntry> dsEntries = build(cotx, entries);

            if (dsEntries != null)
                dataStreamer.addData(dsEntries);

            if ((flags & FLUSH) != 0)
                dataStreamer.flush();

            if ((flags & CLOSE) != 0) {
                dataStreamer.close();
                ctx.resources().release(streamerId);
            }
        }
        catch (IllegalStateException ignored) {
            return getInvalidNodeStateResponse();
        }

        return new ClientResponse(requestId());
    }
}
