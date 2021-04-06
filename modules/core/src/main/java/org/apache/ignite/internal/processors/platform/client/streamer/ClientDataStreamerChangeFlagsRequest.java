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
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Request to change data streamer flags.
 */
public class ClientDataStreamerChangeFlagsRequest extends ClientRequest {
    /** Allow overwrite flag mask. */
    private static final byte ALLOW_OVERWRITE_FLAG_MASK = 0x01;

    /** Skip store flag mask. */
    private static final byte SKIP_STORE_FLAG_MASK = 0x02;

    /** Keep binary flag mask. */
    private static final byte KEEP_BINARY_FLAG_MASK = 0x04;

    /** Data streamer resource id. */
    private final long rsrcId;

    /** Flags. */
    private final byte flags;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientDataStreamerChangeFlagsRequest(BinaryRawReaderEx reader) {
        super(reader);

        rsrcId = reader.readLong();
        flags = reader.readByte();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteDataStreamer<KeyCacheObject, CacheObject> dataStreamer = ctx.resources().get(rsrcId);

        dataStreamer.allowOverwrite((flags & ALLOW_OVERWRITE_FLAG_MASK) != 0);
        dataStreamer.skipStore((flags & SKIP_STORE_FLAG_MASK) != 0);
        dataStreamer.keepBinary((flags & KEEP_BINARY_FLAG_MASK) != 0);

        return new ClientResponse(requestId());
    }
}
