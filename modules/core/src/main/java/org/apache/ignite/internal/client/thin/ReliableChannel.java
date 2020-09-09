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
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Communication channel with failover and partition awareness.
 */
final class ReliableChannel implements AutoCloseable, NotificationListener {
    /** Timeout to wait for executor service to shutdown (in milliseconds). */
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT = 10_000L;

    /** Async runner thread name. */
    static final String ASYNC_RUNNER_THREAD_NAME = "thin-client-channel-async-init";

    /** Channel factory. */
    private final Function<ClientChannelConfiguration, ClientChannel> chFactory;

    /** Client channel holders for each configured address. */
    private final AtomicReference<List<ClientChannelHolder>> channels = new AtomicReference<>();

    /** Index of the current channel. */
    private volatile int curChIdx = -1;

    /** Partition awareness enabled. */
    private final boolean partitionAwarenessEnabled;

    /** Cache partition awareness context. */
    private final ClientCacheAffinityContext affCtx;

    /** Client configuration. */
    private final ClientConfiguration clientCfg;

    /** Node channels. */
    private final Map<UUID, ClientChannelHolder> nodeChannels = new ConcurrentHashMap<>();

    /** Notification listeners. */
    private final Collection<NotificationListener> notificationLsnrs = new CopyOnWriteArrayList<>();

    /** Listeners of channel close events. */
    private final Collection<Consumer<ClientChannel>> channelCloseLsnrs = new CopyOnWriteArrayList<>();

    /** Async tasks thread pool. */
    private final ExecutorService asyncRunner = Executors.newSingleThreadExecutor(
        new ThreadFactory() {
            @Override public Thread newThread(@NotNull Runnable r) {
                Thread thread = new Thread(r, ASYNC_RUNNER_THREAD_NAME);

                thread.setDaemon(true);

                return thread;
            }
        }
    );

    /** Channels reinit was scheduled. */
    private final AtomicBoolean scheduledChannelsReinit = new AtomicBoolean();

    /** Affinity map update is in progress. */
    private final AtomicBoolean affUpdateInProgress = new AtomicBoolean();

    /** Channel is closed. */
    private volatile boolean closed;

    /** Fail (disconnect) listeners. */
    private final ArrayList<Runnable> chFailLsnrs = new ArrayList<>();

    /** Guard channels and curChIdx together. */
    private final ReadWriteLock curChannelsGuard = new ReentrantReadWriteLock();

    /**
     * Constructor.
     */
    ReliableChannel(
        Function<ClientChannelConfiguration, ClientChannel> chFactory,
        ClientConfiguration clientCfg,
        IgniteBinary binary
    ) {
        if (chFactory == null)
            throw new NullPointerException("chFactory");

        if (clientCfg == null)
            throw new NullPointerException("clientCfg");

        this.clientCfg = clientCfg;
        this.chFactory = chFactory;

        partitionAwarenessEnabled = clientCfg.isPartitionAwarenessEnabled();

        affCtx = new ClientCacheAffinityContext(binary);
    }

    /**
     * Establishing connections to servers. If partition awareness feature is enabled connections are created
     * for every configured server. Otherwise only default channel is connected.
     */
    void initConnection() {
        channelsInit(false);
        if (!partitionAwarenessEnabled)
            applyOnDefaultChannel(channel -> {
                // do nothing, just trigger channel connection.
                return null;
            });
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        closed = true;

        asyncRunner.shutdown();

        try {
            asyncRunner.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ignore) {
            // No-op.
        }

        if (channels.get() != null) {
            for (ClientChannelHolder hld: channels.get())
                hld.close();
        }
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
        return applyOnDefaultChannel(channel ->
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
                return apply(affNodeId, channel ->
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
        notificationLsnrs.add(lsnr);
    }

    /**
     * Add listener of channel close event.
     *
     * @param lsnr Listener.
     */
    public void addChannelCloseListener(Consumer<ClientChannel> lsnr) {
        channelCloseLsnrs.add(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void acceptNotification(
        ClientChannel ch,
        ClientOperation op,
        long rsrcId,
        byte[] payload,
        Exception err
    ) {
        for (NotificationListener lsnr : notificationLsnrs) {
            try {
                lsnr.acceptNotification(ch, op, rsrcId, payload, err);
            }
            catch (Exception ignore) {
                // No-op.
            }
        }
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

                        Boolean result = applyOnNodeChannel(nodeId, channel ->
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
     * @return host:port_range address lines parsed as {@link InetSocketAddress}.
     */
    private Set<InetSocketAddress> parseAddresses(String[] addrs) throws ClientException {
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
            .collect(Collectors.toSet());
    }

    /**
     * Roll current default channel if specified holder equals to it.
     */
    private void rollCurrentChannel(ClientChannelHolder hld) {
        curChannelsGuard.writeLock().lock();
        try {
            ClientChannelHolder dfltHld = channels.get().get(curChIdx);
            if (dfltHld == hld) {
                int idx = curChIdx + 1;
                if (idx >= channels.get().size())
                    curChIdx = 0;
                else
                    curChIdx = idx;
            }
        } finally {
            curChannelsGuard.writeLock().unlock();
        }
    }

    /**
     * On channel of the specified holder failure.
     */
    private void onChannelFailure(ClientChannelHolder hld, ClientChannel ch) {
        if (hld != null && ch != null && ch == hld.ch)
            hld.closeChannel();

        rollCurrentChannel(hld);

        chFailLsnrs.forEach(Runnable::run);
    }

    /**
     * Asynchronously try to establish a connection to all configured servers.
     */
    void initAllChannelsAsync() {
        asyncRunner.submit(
            () -> {
                for (ClientChannelHolder hld : channels.get()) {
                    if (stopInitCondition())
                        return; // New reinit task scheduled or channel is closed.

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

    /**
     * Topology version change detected on the channel.
     *
     * @param ch Channel.
     */
    private void onTopologyChanged(ClientChannel ch) {
        if (partitionAwarenessEnabled && affCtx.updateLastTopologyVersion(ch.serverTopologyVersion(), ch.serverNodeId()))
            channelsInit(true);
    }

    /**
     * @param chFailLsnr Listener for the channel fail (disconnect).
     */
    public void addChannelFailListener(Runnable chFailLsnr) {
        chFailLsnrs.add(chFailLsnr);
    }

    /** Should the channel initialization be stopped. */
    private boolean stopInitCondition() {
        return scheduledChannelsReinit.get() || closed;
    }

    /**
     * Init channel holders to all nodes.
     * @param force enable to replace existing channels with new holders.
     */
    private synchronized void initChannelHolders(boolean force) {
        // enable parallel threads to schedule new init of channel holders
        scheduledChannelsReinit.set(false);

        if (!force && channels.get() != null)
            return;

        Set<InetSocketAddress> resolvedAddrs = parseAddresses(clientCfg.getAddresses());

        List<ClientChannelHolder> holders = Optional.ofNullable(channels.get()).orElse(new ArrayList<>());

        // addr -> (holder, delete)
        Map<InetSocketAddress, T2<ClientChannelHolder, Boolean>> addrs = holders.stream()
            .collect(Collectors.toMap(
                c -> c.chCfg.getAddress(),
                c -> new T2<>(c, null)
        ));

        // mark for delete addrs that aren't provided by clientConfig now
        addrs.keySet()
            .stream()
            .filter(addr -> !resolvedAddrs.contains(addr))
            .forEach(addr -> addrs.get(addr).setValue(true));

        // create new holders for new addrs
        resolvedAddrs.stream()
            .filter(addr -> !addrs.containsKey(addr))
            .forEach(addr -> {
                ClientChannelHolder hld = new ClientChannelHolder(new ClientChannelConfiguration(clientCfg, addr));
                addrs.put(addr, new T2<>(hld, false));
            });

        if (!stopInitCondition()) {
            List<ClientChannelHolder> list = new ArrayList<>();
            // The variable holds a new index of default channel after topology change.
            // Suppose that reuse of the channel is better than open new connection.
            int dfltChannelIdx = -1;

            ClientChannelHolder currHolder = null;
            if (curChIdx != -1)
                currHolder = channels.get().get(curChIdx);

            for (T2<ClientChannelHolder, Boolean> t : addrs.values()) {
                ClientChannelHolder hld = t.get1();
                Boolean markForDelete = t.get2();

                if (markForDelete == null) {
                    // this channel is still in use
                    list.add(hld);
                    if (hld == currHolder)
                        dfltChannelIdx = list.size() - 1;

                }
                else if (markForDelete) {
                    // this holder should be deleted now
                    nodeChannels.values().remove(hld);
                    hld.close();
                }
                else {
                    // this channel is new
                    list.add(hld);
                }
            }

            if (dfltChannelIdx == -1)
                dfltChannelIdx = new Random().nextInt(list.size());

            curChannelsGuard.writeLock().lock();
            try {
                channels.set(list);
                curChIdx = dfltChannelIdx;
            } finally {
                curChannelsGuard.writeLock().unlock();
            }
        }
    }

    /** Initialization of channels. */
    private void channelsInit(boolean force) {
        if (!force && channels.get() != null)
            return;

        // Skip if there is already channels reinit scheduled.
        // Flag is set back when a thread comes in synchronized initChannelHolders
        if (scheduledChannelsReinit.compareAndSet(false, true)) {
            initChannelHolders(force);

            if (partitionAwarenessEnabled)
                initAllChannelsAsync();
        }
    }

    /**
     * Apply specified {@code function} on a channel corresponding to specified {@code nodeId}.
     */
    private <T> T applyOnNodeChannel(UUID nodeId, Function<ClientChannel, T> function) {
        ClientChannelHolder hld = null;
        ClientChannel channel = null;

        try {
            hld = nodeChannels.get(nodeId);

            channel = Optional
                .ofNullable(hld)
                .map(ClientChannelHolder::getOrCreateChannel)
                .orElse(null);

            if (channel != null)
                return function.apply(channel);

        } catch (ClientConnectionException e) {
            onChannelFailure(hld, channel);
        }

        return null;
    }

    /**
     * Apply specified {@code function} on any of available channel.
     */
    private <T> T applyOnDefaultChannel(Function<ClientChannel, T> function) {
        List<ClientChannelHolder> holders = channels.get();
        int attemptsLimit = clientCfg.getRetryLimit() > 0 ?
            Math.min(clientCfg.getRetryLimit(), holders.size()) : holders.size();

        ClientConnectionException failure = null;

        for (int attempt = 0; attempt < attemptsLimit; attempt++) {
            ClientChannelHolder hld = null;
            ClientChannel c = null;
            try {
                if (closed)
                    throw new ClientException("Channel is closed");

                curChannelsGuard.readLock().lock();
                try {
                    hld = channels.get().get(curChIdx);
                } finally {
                    curChannelsGuard.readLock().unlock();
                }

                c = hld.getOrCreateChannel();
                if (c != null)
                    return function.apply(c);
            }
            catch (ClientConnectionException e) {
                if (failure == null)
                    failure = e;
                else
                    failure.addSuppressed(e);

                onChannelFailure(hld, c);
            }
        }

        throw failure;
    }

    /**
     * Try apply specified {@code function} on a channel corresponding to {@code tryNodeId}.
     * If failed then apply the function on any available channel.
     */
    private <T> T apply(UUID tryNodeId, Function<ClientChannel, T> function) {
        ClientChannelHolder hld = nodeChannels.get(tryNodeId);

        if (hld != null) {
            ClientChannel channel = null;

            try {
                channel = hld.getOrCreateChannel();
                if (channel != null)
                    return function.apply(channel);

            } catch (ClientConnectionException e) {
                onChannelFailure(hld, channel);
            }
        }

        return applyOnDefaultChannel(function);
    }

    /**
     * Channels holder.
     */
    private class ClientChannelHolder {
        /** Channel configuration. */
        private final ClientChannelConfiguration chCfg;

        /** Channel. */
        private volatile ClientChannel ch;

        /** Address that holder is bind to (chCfg.addr) is not in use now. So close the holder */
        private volatile boolean close;

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
        private ClientChannel getOrCreateChannel()
            throws ClientConnectionException, ClientAuthenticationException, ClientProtocolError {
            return getOrCreateChannel(false);
        }

        /**
         * Get or create channel.
         */
        private ClientChannel getOrCreateChannel(boolean ignoreThrottling)
            throws ClientConnectionException, ClientAuthenticationException, ClientProtocolError {
            if (ch == null && !close) {
                synchronized (this) {
                    if (close)
                        return null;

                    if (ch != null)
                        return ch;

                    if (!ignoreThrottling && applyReconnectionThrottling())
                        throw new ClientConnectionException("Reconnect is not allowed due to applied throttling");

                    ClientChannel channel = chFactory.apply(chCfg);

                    if (channel.serverNodeId() != null) {
                        channel.addTopologyChangeListener(ReliableChannel.this::onTopologyChanged);
                        channel.addNotificationListener(ReliableChannel.this);

                        nodeChannels.values().remove(this);

                        nodeChannels.putIfAbsent(channel.serverNodeId(), this);
                    }

                    ch = channel;
                }
            }

            return ch;
        }

        /**
         * Close channel.
         */
        private synchronized void closeChannel() {
            if (ch != null) {
                U.closeQuiet(ch);

                for (Consumer<ClientChannel> lsnr : channelCloseLsnrs)
                    lsnr.accept(ch);

                ch = null;
            }
        }

        /** Close holder. */
        private void close() {
            close = true;
            closeChannel();
        }
    }
}
