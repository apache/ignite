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

package org.apache.ignite.internal.processors.platform.client.service;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Request topology of certain service.
 */
public class ClientServiceTopologyRequest extends ClientRequest {
    /** The service name. */
    private final String name;

    /**
     * Creates the service topology request.
     *
     * @param reader Reader to read the {@link #name} from.
     */
    public ClientServiceTopologyRequest(BinaryRawReader reader) {
        super(reader);

        name = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        Map<UUID, Integer> srvcTop;

        try {
            srvcTop = ctx.kernalContext().service().serviceTopology(name, 0);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteClientException(ClientStatus.FAILED, "Failed to get topology for service '" + name + "'.", e);
        }

        return new ClientServiceMappingsResponse(requestId(), F.isEmpty(srvcTop) ? Collections.emptyList() : srvcTop.keySet());
    }
}
