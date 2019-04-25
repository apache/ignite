/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.binary.BinaryRawReader;

/**
 * Resource close request.
 */
public class ClientResourceCloseRequest extends ClientRequest {
    /** Resource id. */
    private final long resId;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    ClientResourceCloseRequest(BinaryRawReader reader) {
        super(reader);

        resId = reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        ctx.resources().release(resId);

        return new ClientResponse(requestId());
    }
}
