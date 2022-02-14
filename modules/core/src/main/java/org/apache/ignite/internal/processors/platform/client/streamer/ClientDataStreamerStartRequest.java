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

import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientLongResponse;
import org.apache.ignite.internal.processors.platform.client.ClientPlatform;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRequest;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.stream.StreamReceiver;

import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.ALLOW_OVERWRITE;
import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.CLOSE;
import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.FLUSH;
import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.KEEP_BINARY;
import static org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerFlags.SKIP_STORE;

/**
 * Starts the data streamer.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientDataStreamerStartRequest extends ClientDataStreamerRequest {
    /** */
    private final int cacheId;

    /** */
    private final byte flags;

    /** */
    private final int perNodeBufferSize;

    /** */
    private final int perThreadBufferSize;

    /** Receiver object */
    private final Object receiverObj;

    /** Receiver platform. */
    private final byte receiverPlatform;

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
        receiverObj = reader.readObjectDetached();
        receiverPlatform = receiverObj == null ? 0 : reader.readByte();
        entries = ClientDataStreamerReader.read(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        String cacheName = ClientCacheRequest.cacheDescriptor(ctx, cacheId).cacheName();
        DataStreamerImpl<KeyCacheObject, CacheObject> dataStreamer = (DataStreamerImpl<KeyCacheObject, CacheObject>)
                ctx.kernalContext().grid().<KeyCacheObject, CacheObject>dataStreamer(cacheName);

        try {
            boolean close = (flags & CLOSE) != 0;
            boolean keepBinary = (flags & KEEP_BINARY) != 0;
            boolean flush = (flags & FLUSH) != 0;
            boolean allowOverwrite = (flags & ALLOW_OVERWRITE) != 0;
            boolean skipStore = (flags & SKIP_STORE) != 0;

            // Don't use thread buffer for a one-off streamer operation.
            boolean useThreadBuffer = !close;

            if (perNodeBufferSize >= 0)
                dataStreamer.perNodeBufferSize(perNodeBufferSize);
            else if (entries != null && !entries.isEmpty() && close)
                dataStreamer.perNodeBufferSize(entries.size());

            if (perThreadBufferSize >= 0 && useThreadBuffer)
                dataStreamer.perThreadBufferSize(perThreadBufferSize);

            dataStreamer.allowOverwrite(allowOverwrite);
            dataStreamer.skipStore(skipStore);
            dataStreamer.keepBinary(keepBinary);

            if (receiverObj != null)
                dataStreamer.receiver(createReceiver(ctx.kernalContext(), receiverObj, receiverPlatform, keepBinary));

            if (entries != null)
                dataStreamer.addDataInternal(entries, useThreadBuffer);

            if (flush)
                dataStreamer.flush();

            if (close) {
                dataStreamer.close();

                return new ClientLongResponse(requestId(), 0);
            } else {
                long rsrcId = ctx.resources().put(new ClientDataStreamerHandle(dataStreamer));

                return new ClientLongResponse(requestId(), rsrcId);
            }
        }
        catch (IllegalStateException unused) {
            return getInvalidNodeStateResponse();
        }
    }

    /**
     * Creates the receiver.
     *
     * @param ctx Kernal context.
     * @param receiverObj Receiver.
     * @param platform Platform code.
     * @param keepBinary Keep binary flag.
     * @return Receiver.
     */
    private static StreamReceiver createReceiver(GridKernalContext ctx,
                                                 Object receiverObj,
                                                 byte platform,
                                                 boolean keepBinary) {
        if (receiverObj == null)
            return null;

        switch (platform) {
            case ClientPlatform.JAVA:
                return ((BinaryObject)receiverObj).deserialize();

            case ClientPlatform.DOTNET:
                PlatformContext platformCtx = ctx.platform().context();

                String curPlatform = platformCtx.platform();

                if (!PlatformUtils.PLATFORM_DOTNET.equals(curPlatform)) {
                    throw new IgniteException("Stream receiver platform is " + PlatformUtils.PLATFORM_DOTNET +
                            ", current platform is " + curPlatform);
                }

                return platformCtx.createStreamReceiver(receiverObj, 0, keepBinary);

            case ClientPlatform.CPP:

            default:
                throw new UnsupportedOperationException("Invalid stream receiver platform code: " + platform);
        }
    }
}
