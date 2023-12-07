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

package org.apache.ignite.internal.processors.platform.client.datastructures;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicLongImpl;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;

/**
 * Atomic long value request.
 */
public class ClientAtomicLongRequest extends ClientRequest {
    /** Atomic long name. */
    private final String name;

    /** Cache group name. */
    private final String groupName;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientAtomicLongRequest(BinaryRawReader reader) {
        super(reader);

        name = reader.readString();
        groupName = reader.readString();
    }

    /**
     * Gets the atomic long.
     *
     * @param ctx Context.
     * @return Atomic long or null.
     */
    protected GridCacheAtomicLongImpl atomicLong(ClientConnectionContext ctx) {
        AtomicConfiguration cfg = groupName == null ? null : new AtomicConfiguration().setGroupName(groupName);

        try {
            return (GridCacheAtomicLongImpl)ctx.kernalContext().dataStructures().atomicLong(name, cfg, 0, false);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /**
     * Gets a response for non-existent atomic long.
     *
     * @return Response for non-existent atomic long.
     */
    protected ClientResponse notFoundResponse() {
        return new ClientResponse(
                requestId(),
                ClientStatus.RESOURCE_DOES_NOT_EXIST,
                String.format("AtomicLong with name '%s' does not exist.", name));
    }
}
