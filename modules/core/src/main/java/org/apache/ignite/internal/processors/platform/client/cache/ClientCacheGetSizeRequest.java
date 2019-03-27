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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientLongResponse;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxAwareRequest;

/**
 * Cache size request.
 */
public class ClientCacheGetSizeRequest extends ClientCacheDataRequest implements ClientTxAwareRequest {
    /** Peek modes. */
    private final CachePeekMode[] modes;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientCacheGetSizeRequest(BinaryRawReader reader) {
        super(reader);

        int cnt = reader.readInt();

        modes = new CachePeekMode[cnt];

        for (int i = 0; i < cnt; i++) {
            modes[i] = CachePeekMode.fromOrdinal(reader.readByte());
        }
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        long res = cache(ctx).sizeLong(modes);

        return new ClientLongResponse(requestId(), res);
    }
}
