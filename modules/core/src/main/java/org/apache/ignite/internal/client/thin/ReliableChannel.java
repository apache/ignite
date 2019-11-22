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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Communication channel with failover and affinity awareness.
 */
final class ReliableChannel implements AutoCloseable {
    /** Channel factory. */
    private final Function<ClientChannelConfiguration, ClientChannel> chFactory;

    /** Client channel holders for each configured address. */
    private final ClientChannelHolder[] channels;

    /** Index of the current channel. */
    private int curChIdx;

    /** Affinity awareness enabled. */
    private final boolean affinityAwarenessEnabled;

    /** Cache affinity awareness context. */
    private final ClientCacheAffinityContext affinityCtx;

    /** Node channels. */
    private final Map<UUID, ClientChannelHolder> nodeChannels = new ConcurrentHashMap<>();

    /** Async tasks thread pool. */
    private final ExecutorService asyncRunner = Executors.newSingleThreadExecutor(
        new ThreadFactory() {
            @Override public Thread newThread(@NotNull Runnable r) {
                return new Thread(r, "thin-client-channel-async-runner");
            }
        }
    );

    /** Channels reinit was scheduled. */
    private final AtomicBoolean scheduledChannelsReinit = new AtomicBoolean();

    /** Affinity map update is in progress. */
    private final AtomicBoolean affinityUpdateInProgress = new AtomicBoolean();

    /** Channel is closed. */
    private boolean closed;

    /**
     * Constructor.
     */
    ReliableChannel(
        Function<ClientChannelConfiguration, ClientChannel> chFactory,
        ClientConfiguration clientCfg,
        IgniteBinary binary
    ) throws ClientException {
        if (chFactory == null)
            throw new NullPointerException("chFactory");

        if (clientCfg == null)
            throw new NullPointerException("clientCfg");

        this.chFactory = chFactory;

        List<InetSocketAddress> addrs = parseAddresses(clientCfg.getAddresses());

        channels = new ClientChannelHolder[addrs.size()];

        for (int i = 0; i < channels.length; i++)
            channels[i] = new ClientChannelHolder(new ClientChannelConfiguration(clientCfg, addrs.get(i)));

        curChIdx = new Random().nextInt(channels.length); // We already verified there is at least one address.

        affinityAwarenessEnabled = clientCfg.isAffinityAwarenessEnabled() && channels.length > 1;

        affinityCtx = new ClientCacheAffinityContext(binary);

        ClientConnectionException lastEx = null;

        for (int i = 0; i < channels.length; i++) {
            try {
                channels[curChIdx].getOrCreateChannel();

                if (affinityAwarenessEnabled)
                    initAllChannelsAsync();

                return;
            } catch (ClientConnectionException e) {
                lastEx = e;

                rollCurrentChannel();
            }
        }

        throw lastEx;
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        closed = true;

        for (ClientChannelHolder hld : channels)
            hld.closeChannel();
    }

    /**
     * Send request and handle response.
     */
    public <T> T service(
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException {
        ClientConnectionException failure = null;

        for (int i = 0; i < channels.length; i++) {
            ClientChannel ch = null;

            try {
                ch = channel();

                return ch.service(op, payloadWriter, payloadReader);
            }
            catch (ClientConnectionException e) {
                if (failure == null)
                    failure = e;
                else
                    failure.addSuppressed(e);

                onChannelFailure(ch);
            }
        }

        throw failure;
    }

    /**
     * Send request without payload and handle response.
     */
    public <T> T service(ClientOperation op, Function<PayloadInputChannel, T> payloadReader)
        throws ClientException {
        return service(op, null, payloadReader);
    }

    /**
     * Send request and handle response without payload.
     */
    public void request(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter) throws ClientException {
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
    ) throws ClientException {
        if (affinityAwarenessEnabled && !nodeChannels.isEmpty() && affinityInfoIsUpToDate(cacheId)) {
            UUID affinityNodeId = affinityCtx.affinityNode(cacheId, key);

            if (affinityNodeId != null) {
                ClientChannelHolder hld = nodeChannels.get(affinityNodeId);

                if (hld != null) {
                    ClientChannel ch = null;

                    try {
                        ch = hld.getOrCreateChannel();

                        return ch.service(op, payloadWriter, payloadReader);
                    }
                    catch (ClientConnectionException ignore) {
                        onChannelFailure(hld, ch);
                    }
                }
            }
        }

        // Can't determine affinity node or request to affinity node failed - proceed with standart failover service.
        return service(op, payloadWriter, payloadReader);
    }

    /**
     * Checks if affinity information for the cache is up to date and tries to update it if not.
     *
     * @return {@code True} if affinity information is up to date, {@code false} if there is not affinity information
     * available for this cache or information is obsolete and failed to update it.
     */
    private boolean affinityInfoIsUpToDate(int cacheId) {
        if (affinityCtx.affinityUpdateRequired(cacheId)) {
            if (affinityUpdateInProgress.compareAndSet(false, true)) {
                try {
                    ClientCacheAffinityContext.TopologyNodes lastTop = affinityCtx.lastTopology();

                    if (lastTop == null)
                        return false;

                    for (UUID nodeId : lastTop.nodes()) {
                        // Abort iterations when topology changed.
                        if (lastTop != affinityCtx.lastTopology())
                            return false;

                        ClientChannelHolder hld = nodeChannels.get(nodeId);

                        if (hld != null) {
                            ClientChannel ch = null;

                            try {
                                ch = hld.getOrCreateChannel();

                                return ch.service(ClientOperation.CACHE_PARTITIONS,
                                    affinityCtx::writePartitionsUpdateRequest,
                                    affinityCtx::readPartitionsUpdateResponse);
                            }
                            catch (ClientConnectionException ignore) {
                                onChannelFailure(hld, ch);
                            }
                        }
                    }

                    // There is no one alive node found for last topology version, we should reset affinity context
                    // to let affinity get updated in case of reconnection to the new cluster (with lower topology
                    // version).
                    affinityCtx.reset(lastTop);
                }
                finally {
                    affinityUpdateInProgress.set(false);
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
     * @return host:port_range address lines parsed as {@link InetSocketAddress}.
     */
    private static List<InetSocketAddress> parseAddresses(String[] addrs) throws ClientException {
        if (F.isEmpty(addrs))
            throw new ClientException("Empty addresses");

        Collection<HostAndPortRange> ranges = new ArrayList<>(addrs.length);

        for (String a : addrs) {
            try {
                ranges.add(HostAndPortRange.parse(
                    a,
                    ClientConnectorConfiguration.DFLT_PORT,
                    ClientConnectorConfiguration.DFLT_PORT + ClientConnectorConfiguration.DFLT_PORT_RANGE,
                    "Failed to parse Ignite server address"
                ));
            }
            catch (IgniteCheckedException e) {
                throw new ClientException(e);
            }
        }

        return ranges.stream()
            .flatMap(r -> IntStream
                .rangeClosed(r.portFrom(), r.portTo()).boxed()
                .map(p -> new InetSocketAddress(r.host(), p))
            )
            .collect(Collectors.toList());
    }

    /** */
    private synchronized ClientChannel channel() {
        if (closed)
            throw new ClientException("Channel is closed");

        try {
            return channels[curChIdx].getOrCreateChannel();
        }
        catch (ClientConnectionException e) {
            rollCurrentChannel();

            throw e;
        }
    }

    /** */
    private synchronized void rollCurrentChannel() {
        if (++curChIdx >= channels.length)
            curChIdx = 0;
    }

    /**
     * On current channel failure.
     */
    private synchronized void onChannelFailure(ClientChannel ch) {
        // There is nothing wrong if curChIdx was concurrently changed, since channel was closed by another thread
        // when current index was changed and no other wrong channel will be closed by current thread because
        // onChannelFailure checks channel binded to the holder before closing it.
        onChannelFailure(channels[curChIdx], ch);
    }

    /**
     * On channel of the specified holder failure.
     */
    private synchronized void onChannelFailure(ClientChannelHolder hld, ClientChannel ch) {
        if (ch == hld.ch && ch != null) {
            hld.closeChannel();

            if (hld == channels[curChIdx])
                rollCurrentChannel();
        }
    }

    /**
     * Asynchronously try to establish a connection to all configured servers.
     */
    private void initAllChannelsAsync() {
        // Skip if there is already channels reinit scheduled.
        if (scheduledChannelsReinit.compareAndSet(false, true)) {
            asyncRunner.submit(
                () -> {
                    scheduledChannelsReinit.set(false);

                    for (ClientChannelHolder hld : channels) {
                        if (scheduledChannelsReinit.get())
                            return; // New reinit task scheduled.

                        try {
                            hld.getOrCreateChannel(true);
                        }
                        catch (Exception ignore) {
                            // No-op.
                        }
                    }
                }
            );
        }
    }

    /**
     * Topology version change detected on the channel.
     *
     * @param ch Channel.
     */
    private void onTopologyChanged(ClientChannel ch) {
        if (affinityAwarenessEnabled && affinityCtx.updateLastTopologyVersion(ch.serverTopologyVersion(),
            ch.serverNodeId()))
            initAllChannelsAsync();
    }

    /**
     * Channels holder.
     */
    private class ClientChannelHolder {
        /** Channel configuration. */
        private final ClientChannelConfiguration chCfg;

        /** Channel. */
        private volatile ClientChannel ch;

        /** Timestamps of reconnect retries. */
        private final long[] reconnectRetries;

        /**
         * @param chCfg Channel config.
         */
        private ClientChannelHolder(ClientChannelConfiguration chCfg) {
            this.chCfg = chCfg;

            reconnectRetries = chCfg.getReconnectThrottlingRetries() > 0 && chCfg.getReconnectThrottlingPeriod() > 0L ?
                new long[chCfg.getReconnectThrottlingRetries()] : null;
        }

        /**
         * @return Whether reconnect throttling should be applied.
         */
        private boolean applyReconnectionThrottling() {
            if (reconnectRetries == null)
                return false;

            long ts = System.currentTimeMillis();

            for (int i = 0; i < reconnectRetries.length; i++) {
                if (ts - reconnectRetries[i] >= chCfg.getReconnectThrottlingPeriod()) {
                    reconnectRetries[i] = ts;

                    return false;
                }
            }

            return true;
        }

        /**
         * Get or create channel.
         */
        private synchronized ClientChannel getOrCreateChannel() {
            return getOrCreateChannel(false);
        }

        /**
         * Get or create channel.
         */
        private synchronized ClientChannel getOrCreateChannel(boolean ignoreThrottling) {
            if (ch == null) {
                if (!ignoreThrottling && applyReconnectionThrottling())
                    throw new ClientConnectionException("Reconnect is not allowed due to applied throttling");

                ch = chFactory.apply(chCfg);

                if (ch.serverNodeId() != null) {
                    ch.addTopologyChangeListener(ReliableChannel.this::onTopologyChanged);

                    nodeChannels.values().remove(this);

                    nodeChannels.putIfAbsent(ch.serverNodeId(), this);
                }
            }

            return ch;
        }

        /**
         * Close channel.
         */
        private synchronized void closeChannel() {
            U.closeQuiet(ch);

            ch = null;
        }
    }
}
