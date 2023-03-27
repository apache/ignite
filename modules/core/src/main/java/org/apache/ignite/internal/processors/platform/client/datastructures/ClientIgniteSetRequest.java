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

import org.apache.ignite.IgniteSet;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;

/**
 * Ignite set get or update request.
 */
public class ClientIgniteSetRequest extends ClientRequest {
    /** */
    private final String name;

    /** */
    private final int cacheId;

    /** */
    private final boolean collocated;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientIgniteSetRequest(BinaryRawReader reader) {
        super(reader);

        name = reader.readString();
        cacheId = reader.readInt();
        collocated = reader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteSet<Object> igniteSet = igniteSet(ctx);

        if (igniteSet == null)
            return notFoundResponse();

        return process(igniteSet);
    }

    /**
     * Processes the request.
     *
     * @param set Ignite set.
     * @return Response.
     */
    protected ClientResponse process(IgniteSet<Object> set) {
        return new ClientResponse(requestId());
    }

    /**
     * Gets the name.
     *
     * @return Set name.
     */
    protected String name() {
        return name;
    }

    /**
     * Gets the IgniteSet.
     *
     * @param ctx Context.
     * @return IgniteSet or null.
     */
    protected <T> IgniteSet<T> igniteSet(ClientConnectionContext ctx) {
        // Thin client only works in separated mode, because non-separated mode was discontinued earlier.
        IgniteSet<T> set = ctx.kernalContext().grid().set(name, cacheId, collocated, true);

        if (set != null) {
            set = set.withKeepBinary();
        }

        return set;
    }

    /**
     * Gets a response for non-existent set.
     *
     * @return Response for non-existent set.
     */
    protected ClientResponse notFoundResponse() {
        return new ClientResponse(
                requestId(),
                ClientStatus.RESOURCE_DOES_NOT_EXIST,
                String.format("IgniteSet with name '%s' does not exist.", name));
    }
}
