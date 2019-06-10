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
 * Custom query request.
 */
public class ClientCustomQueryRequest extends ClientRequest {
    /** Reader. */
    private final BinaryRawReader reader;

    /** Processor id. */
    private final String processorId;

    /** Operation id. */
    private final byte methodId;

    /**
     * Creates an instance of request.
     *
     * @param reader Reader.
     */
    public ClientCustomQueryRequest(BinaryRawReader reader) {
        super(reader);

        this.processorId = reader.readString();
        this.methodId = reader.readByte();
        this.reader = reader;
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        return ThinClientCustomQueryRegistry.call(requestId(), processorId, methodId, reader);
    }
}
