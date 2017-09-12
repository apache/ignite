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

import org.apache.ignite.binary.BinaryRawReader;

/**
 * Resource close request.
 */
public class ClientResourceCloseRequest extends ClientRequest {
    /** Cursor id. */
    private final long resourceId;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    ClientResourceCloseRequest(BinaryRawReader reader) {
        super(reader);

        resourceId = reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        ClientCloseableResource cur = ctx.handleRegistry().get(resourceId);

        cur.close();
        ctx.handleRegistry().release(resourceId);

        return new ClientResponse(getRequestId());
    }
}
