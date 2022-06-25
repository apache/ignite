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

import java.util.UUID;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetProxy;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Ignite set get or update request.
 */
public class ClientIgniteSetRequest extends ClientRequest {
    /** */
    private final String name;

    /** */
    private final String groupName;

    /** */
    private final IgniteUuid id;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientIgniteSetRequest(BinaryRawReader reader) {
        super(reader);

        name = reader.readString();
        groupName = reader.readString();
        id = new IgniteUuid(new UUID(reader.readLong(), reader.readLong()), reader.readLong());
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
        IgniteSet<T> set = ctx.kernalContext().grid().setNoCreate(name, groupName);

        if (set != null && ((GridCacheSetProxy<T>) set).delegate().id().equals(id))
            return set;

        return null;
    }

    /**
     * Gets a response for non-existent set.
     *
     * @return Response for non-existent set.
     */
    protected ClientResponse notFoundResponse() {
        // TODO: Include ID in the message.
        return new ClientResponse(
                requestId(),
                ClientStatus.RESOURCE_DOES_NOT_EXIST,
                String.format("IgniteSet with name '%s' does not exist.", name));
    }
}
