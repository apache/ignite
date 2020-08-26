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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.ClientConfiguration;


/**
 * Communication channel with failover and partition awareness.
 */
final class ReliableChannelFacade implements AutoCloseable {
    /** Partition awareness enabled. */
    private final boolean partitionAwarenessEnabled;

    /** Cache partition awareness context. */
    private final ClientCacheAffinityContext affCtx;

    /** */
    private final ReliableChannel channels;

    /** Affinity map update is in progress. */
    private final AtomicBoolean affUpdateInProgress = new AtomicBoolean();

    /**ß
     * Constructor.
     */
    ReliableChannelFacade(
        Function<ClientChannelConfiguration, ClientChannel> chFactory,
        ClientConfiguration clientCfg,
        IgniteBinary binary
    ) throws ClientException {
        partitionAwarenessEnabled = clientCfg.isPartitionAwarenessEnabled();

        affCtx = new ClientCacheAffinityContext(binary);

        channels = new ReliableChannel(clientCfg, chFactory, partitionAwarenessEnabled);
        channels.addTopologyChangeListener(channel ->
            affCtx.updateLastTopologyVersion(channel.serverTopologyVersion(), channel.serverNodeId())
        );
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        channels.close();
    }

    /**
     * Send request and handle response.
     *
     * @throws ClientException Thrown by {@code payloadWriter} or {@code payloadReader}.
     * @throws ClientAuthenticationException When user name or password is invalid.
     * @throws ClientAuthorizationException When user has no permission to perform operation.
     * @throws ClientProtocolError When failed to handshake with server.
     * @throws ClientServerError When failed to process request on server.
     */
    public <T> T service(
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError {
        return channels.applyOnDefaultChannel(channel ->
            channel.service(op, payloadWriter, payloadReader)
        );
    }

    /**
     * Send request without payload and handle response.
     */
    public <T> T service(ClientOperation op, Function<PayloadInputChannel, T> payloadReader)
        throws ClientException, ClientError {
        return service(op, null, payloadReader);
    }

    /**
     * Send request and handle response without payload.
     */
    public void request(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter)
        throws ClientException, ClientError {
        service(op, payloadWriter, null);
    }

    /**
     * Send request to affinity node and handle response.
     */
    public <T> T affinityService(
        int cacheId,
        Object key,
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError {
        if (partitionAwarenessEnabled && affinityInfoIsUpToDate(cacheId)) {
            UUID affNodeId = affCtx.affinityNode(cacheId, key);

            if (affNodeId != null) {
                return channels.apply(affNodeId, channel ->
                    channel.service(op, payloadWriter, payloadReader)
                );
            }
        }

        return service(op, payloadWriter, payloadReader);
    }

    /**
     * Add notification listener.
     *
     * @param lsnr Listener.
     */
    public void addNotificationListener(NotificationListener lsnr) {
        channels.addNotificationListener(lsnr);
    }

    /**
     * Add listener of channel close event.
     *
     * @param lsnr Listener.
     */
    public void addChannelCloseListener(Consumer<ClientChannel> lsnr) {
        channels.addChannelCloseListener(lsnr);
    }

    /**
     * Checks if affinity information for the cache is up to date and tries to update it if not.
     *
     * @return {@code True} if affinity information is up to date, {@code false} if there is not affinity information
     * available for this cache or information is obsolete and failed to update it.
     */
    private boolean affinityInfoIsUpToDate(int cacheId) {
        if (affCtx.affinityUpdateRequired(cacheId)) {
            if (affUpdateInProgress.compareAndSet(false, true)) {
                try {
                    ClientCacheAffinityContext.TopologyNodes lastTop = affCtx.lastTopology();

                    if (lastTop == null)
                        return false;

                    for (UUID nodeId : lastTop.nodes()) {
                        // Abort iterations when topology changed.
                        if (lastTop != affCtx.lastTopology())
                            return false;

                        Boolean result = channels.applyOnNodeChannel(nodeId, channel ->
                            channel.service(ClientOperation.CACHE_PARTITIONS,
                                affCtx::writePartitionsUpdateRequest,
                                affCtx::readPartitionsUpdateResponse)
                        );

                        if (result != null)
                            return result;
                    }

                    // There is no one alive node found for last topology version, we should reset affinity context
                    // to let affinity get updated in case of reconnection to the new cluster (with lower topology
                    // version).
                    affCtx.reset(lastTop);
                }
                finally {
                    affUpdateInProgress.set(false);
                }
            }

            // No suitable nodes found to update affinity, failed to execute service on all nodes or update is already
            // in progress by another thread.
            return false;
        }
        else
            return true;
    }

    /**
     * @param chFailLsnr Listener for the channel fail (disconnect).
     */
    public void addChannelFailListener(Runnable chFailLsnr) {
        channels.addChannelFailListener(chFailLsnr);
    }
}
