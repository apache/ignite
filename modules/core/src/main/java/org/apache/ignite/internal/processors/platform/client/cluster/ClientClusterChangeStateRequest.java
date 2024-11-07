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

package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import static org.apache.ignite.internal.processors.platform.client.ClientBitmaskFeature.FORCE_DEACTIVATION_FLAG;

/**
 * Cluster status request.
 */
public class ClientClusterChangeStateRequest extends ClientRequest {
    /** Next state. */
    private final ClusterState state;

    /** If {@code true}, cluster deactivation will be forced. */
    private final boolean forceDeactivation;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientClusterChangeStateRequest(BinaryRawReader reader, ClientProtocolContext protocolCtx) {
        super(reader);

        state = ClusterState.fromOrdinal(reader.readByte());
        forceDeactivation = !protocolCtx.isFeatureSupported(FORCE_DEACTIVATION_FLAG) || reader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        try {
            ctx.kernalContext().state().changeGlobalState(
                state,
                forceDeactivation,
                ctx.kernalContext().cluster().get().forServers().nodes(),
                false
            ).get();

            return new ClientResponse(requestId());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
