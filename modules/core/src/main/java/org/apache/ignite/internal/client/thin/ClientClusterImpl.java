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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.client.ClientCluster;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;

/**
 * Implementation of {@link ClientCluster}.
 */
class ClientClusterImpl extends ClientClusterGroupImpl implements ClientCluster {
    /** Default cluster group. */
    private final ClientClusterGroupImpl dfltClusterGrp;

    /**
     * Constructor.
     */
    ClientClusterImpl(ReliableChannel ch, ClientBinaryMarshaller marsh) {
        super(ch, marsh);

        dfltClusterGrp = (ClientClusterGroupImpl)forServers();
    }

    /** {@inheritDoc} */
    @Override public ClusterState state() {
        try {
            return ch.service(ClientOperation.CLUSTER_GET_STATE,
                req -> checkClusterApiSupported(req.clientChannel().protocolCtx()),
                res -> ClusterState.fromOrdinal(res.in().readByte())
            );
        }
        catch (ClientError e) {
            throw new ClientException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void state(ClusterState newState) throws ClientException {
        try {
            ch.service(ClientOperation.CLUSTER_CHANGE_STATE,
                req -> {
                    ProtocolContext protocolCtx = req.clientChannel().protocolCtx();

                    checkClusterApiSupported(protocolCtx);

                    if (newState.ordinal() > 1 && !protocolCtx.isFeatureSupported(ProtocolBitmaskFeature.CLUSTER_STATES)) {
                        throw new ClientFeatureNotSupportedByServerException("State " + newState.name() + " is not " +
                            "supported by the server");
                    }

                    req.out().writeByte((byte)newState.ordinal());
                },
                null
            );
        }
        catch (ClientError e) {
            throw new ClientException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean disableWal(String cacheName) throws ClientException {
        return changeWalState(cacheName, false);
    }

    /** {@inheritDoc} */
    @Override public boolean enableWal(String cacheName) throws ClientException {
        return changeWalState(cacheName, true);
    }

    /** {@inheritDoc} */
    @Override public boolean isWalEnabled(String cacheName) {
        try {
            return ch.service(ClientOperation.CLUSTER_GET_WAL_STATE,
                req -> {
                    checkClusterApiSupported(req.clientChannel().protocolCtx());

                    try (BinaryRawWriterEx writer = utils.createBinaryWriter(req.out())) {
                        writer.writeString(cacheName);
                    }
                },
                res -> res.in().readBoolean()
            );
        }
        catch (ClientError e) {
            throw new ClientException(e);
        }
    }

    /**
     * @param cacheName Cache name.
     * @param enable {@code True} if WAL should be enabled, {@code false} if WAL should be disabled.
     */
    private boolean changeWalState(String cacheName, boolean enable) throws ClientException {
        try {
            return ch.service(ClientOperation.CLUSTER_CHANGE_WAL_STATE,
                req -> {
                    checkClusterApiSupported(req.clientChannel().protocolCtx());

                    try (BinaryRawWriterEx writer = utils.createBinaryWriter(req.out())) {
                        writer.writeString(cacheName);
                        writer.writeBoolean(enable);
                    }
                },
                res -> res.in().readBoolean()
            );
        }
        catch (ClientError e) {
            throw new ClientException(e);
        }
    }

    /**
     * Check that Cluster API is supported by server.
     *
     * @param protocolCtx Protocol context.
     */
    private void checkClusterApiSupported(ProtocolContext protocolCtx)
        throws ClientFeatureNotSupportedByServerException {
        if (!protocolCtx.isFeatureSupported(ProtocolVersionFeature.CLUSTER_API) &&
            !protocolCtx.isFeatureSupported(ProtocolBitmaskFeature.CLUSTER_STATES))
            throw new ClientFeatureNotSupportedByServerException(ProtocolBitmaskFeature.CLUSTER_STATES);
    }

    /**
     * Default cluster group ("for servers").
     */
    ClientClusterGroupImpl defaultClusterGroup() {
        return dfltClusterGrp;
    }
}
