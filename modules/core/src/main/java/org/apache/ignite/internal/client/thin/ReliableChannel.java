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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientOperationType;
import org.apache.ignite.client.ClientPartitionAwarenessMapperFactory;
import org.apache.ignite.client.ClientRetryPolicy;
import org.apache.ignite.client.ClientRetryPolicyContext;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.client.thin.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.thin.io.gridnioserver.GridNioClientConnectionMultiplexer;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;

/**
 * Communication channel with failover and partition awareness.
 */
final class ReliableChannel implements AutoCloseable {
    /** Do nothing helper function. */
    private static final Consumer<Integer> DO_NOTHING = (v) -> {};

    /** Channel factory. */
    private final BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory;

    /** Client channel holders for each configured address. */
    private volatile List<ClientChannelHolder> channels;

    /** Index of the current channel. */
    private volatile int curChIdx = -1;

    /** Partition awareness enabled. */
    private final boolean partitionAwarenessEnabled;

    /** Cache partition awareness context. */
    private final ClientCacheAffinityContext affinityCtx;

    /** Client configuration. */
    private final ClientConfiguration clientCfg;

    /** Logger. */
    private final IgniteLogger log;

    /** Node channels. */
    private final Map<UUID, ClientChannelHolder> nodeChannels = new ConcurrentHashMap<>();

    /** Channels reinit was scheduled. */
    private final AtomicBoolean scheduledChannelsReinit = new AtomicBoolean();

    /** Timestamp of start of channels reinitialization. */
    private volatile long startChannelsReInit;

    /** Timestamp of finish of channels reinitialization. */
    private volatile long finishChannelsReInit;

    /** Affinity map update is in progress. */
    private final AtomicBoolean affinityUpdateInProgress = new AtomicBoolean();

    /** Channel is closed. */
    private volatile boolean closed;

    /** Fail (disconnect) listeners. */
    private final ArrayList<Runnable> chFailLsnrs = new ArrayList<>();

    /** Guard channels and curChIdx together. */
    private final ReadWriteLock curChannelsGuard = new ReentrantReadWriteLock();

    /** Connection manager. */
    private final ClientConnectionMultiplexer connMgr;

    /** Cache addresses returned by {@code ThinClientAddressFinder}. */
    private volatile String[] prevHostAddrs;

    /** Open channels counter. */
    private final AtomicInteger channelsCnt = new AtomicInteger();

    /**
     * Constructor.
     */
    ReliableChannel(
            BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory,
            ClientConfiguration clientCfg,
            IgniteBinary binary
    ) {
        if (chFactory == null)
            throw new NullPointerException("chFactory");

        if (clientCfg == null)
            throw new NullPointerException("clientCfg");

        this.clientCfg = clientCfg;
        this.chFactory = chFactory;
        log = NullLogger.whenNull(clientCfg.getLogger());

        partitionAwarenessEnabled = clientCfg.isPartitionAwarenessEnabled();

        affinityCtx = new ClientCacheAffinityContext(binary, clientCfg.getPartitionAwarenessMapperFactory());

        connMgr = new GridNioClientConnectionMultiplexer(clientCfg);
        connMgr.start();

        if (log.isDebugEnabled())
            log.debug("ReliableChannel created");
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        if (log.isDebugEnabled())
            log.debug("ReliableChannel stopping");

        closed = true;

        connMgr.stop();

        List<ClientChannelHolder> holders = channels;

        if (holders != null) {
            for (ClientChannelHolder hld: holders)
                hld.close();
        }

        if (log.isDebugEnabled())
            log.debug("ReliableChannel stopped");
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
        return applyOnDefaultChannel(channel -> channel.service(op, payloadWriter, payloadReader), op);
    }

    /**
     * Send request and handle response asynchronously.
     */
    public <T> IgniteClientFuture<T> serviceAsync(
            ClientOperation op,
            Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError {
        CompletableFuture<T> fut = new CompletableFuture<>();

        // Use the only one attempt to avoid blocking async method.
        handleServiceAsync(fut, op, payloadWriter, payloadReader, 1, null);

        return new IgniteClientFutureImpl<>(fut);
    }

    /**
     * Handles serviceAsync results and retries as needed.
     */
    private <T> void handleServiceAsync(final CompletableFuture<T> fut,
                                        ClientOperation op,
                                        Consumer<PayloadOutputChannel> payloadWriter,
                                        Function<PayloadInputChannel, T> payloadReader,
                                        int attemptsLimit,
                                        ClientConnectionException failure) {
        ClientChannel ch;
        // Workaround to store used attempts value within lambda body.
        int attemptsCnt[] = new int[1];

        try {
            ch = applyOnDefaultChannel(channel -> channel, null, attemptsLimit, v -> attemptsCnt[0] = v);
        }
        catch (Throwable ex) {
            if (failure != null) {
                failure.addSuppressed(ex);

                fut.completeExceptionally(failure);

                return;
            }

            fut.completeExceptionally(ex);

            return;
        }

        ch
            .serviceAsync(op, payloadWriter, payloadReader)
            .handle((res, err) -> {
                if (err == null) {
                    fut.complete(res);

                    return null;
                }

                ClientConnectionException failure0 = failure;

                if (err instanceof ClientConnectionException) {
                    try {
                        // Will try to reinit channels if topology changed.
                        onChannelFailure(ch, err);
                    }
                    catch (Throwable ex) {
                        fut.completeExceptionally(ex);

                        return null;
                    }

                    if (failure0 == null)
                        failure0 = (ClientConnectionException)err;
                    else
                        failure0.addSuppressed(err);

                    int attempt = attemptsCnt[0];
                    int leftAttempts = attemptsLimit - attempt;

                    // If it is a first retry then reset attempts (as for initialization we use only 1 attempt).
                    if (failure == null)
                        leftAttempts = getRetryLimit() - 1;

                    if (leftAttempts > 0 && shouldRetry(op, attempt, failure0)) {
                        handleServiceAsync(fut, op, payloadWriter, payloadReader, leftAttempts, failure0);

                        return null;
                    }
                }
                else {
                    fut.completeExceptionally(err instanceof ClientException ? err : new ClientException(err));

                    return null;
                }

                fut.completeExceptionally(failure0);

                return null;
            });
    }

    /**
     * Send request without payload and handle response.
     */
    public <T> T service(ClientOperation op, Function<PayloadInputChannel, T> payloadReader)
            throws ClientException, ClientError {
        return service(op, null, payloadReader);
    }

    /**
     * Send request without payload and handle response asynchronously.
     */
    public <T> IgniteClientFuture<T> serviceAsync(ClientOperation op, Function<PayloadInputChannel, T> payloadReader)
        throws ClientException, ClientError {
        return serviceAsync(op, null, payloadReader);
    }

    /**
     * Send request and handle response without payload.
     */
    public void request(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter)
        throws ClientException, ClientError {
        service(op, payloadWriter, null);
    }

    /**
     * Send request and handle response without payload.
     */
    public IgniteClientFuture<Void> requestAsync(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter)
        throws ClientException, ClientError {
        return serviceAsync(op, payloadWriter, null);
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
            UUID affNodeId = affinityCtx.affinityNode(cacheId, key);

            if (affNodeId != null) {
                return applyOnNodeChannelWithFallback(affNodeId, channel ->
                    channel.service(op, payloadWriter, payloadReader), op);
            }
        }

        return service(op, payloadWriter, payloadReader);
    }

    /**
     * Send request to affinity node and handle response.
     */
    public <T> T affinityService(
        int cacheId,
        int part,
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError {
        if (partitionAwarenessEnabled && affinityInfoIsUpToDate(cacheId)) {
            UUID affNodeId = affinityCtx.affinityNode(cacheId, part);

            if (affNodeId != null) {
                return applyOnNodeChannelWithFallback(affNodeId, channel ->
                    channel.service(op, payloadWriter, payloadReader), op);
            }
        }

        return service(op, payloadWriter, payloadReader);
    }

    /**
     * Send request to affinity node and handle response.
     */
    public <T> IgniteClientFuture<T> affinityServiceAsync(
        int cacheId,
        Object key,
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError {
        if (partitionAwarenessEnabled && affinityInfoIsUpToDate(cacheId)) {
            UUID affNodeId = affinityCtx.affinityNode(cacheId, key);

            if (affNodeId != null) {
                CompletableFuture<T> fut = new CompletableFuture<>();

                Object result = applyOnNodeChannel(affNodeId, channel ->
                    channel
                        .serviceAsync(op, payloadWriter, payloadReader)
                        .handle((res, err) -> {
                            if (err == null) {
                                fut.complete(res);
                                return null;
                            }

                            try {
                                // Will try to reinit channels if topology changed.
                                onChannelFailure(channel, err);
                            }
                            catch (Throwable ex) {
                                fut.completeExceptionally(ex);
                                return null;
                            }

                            if (err instanceof ClientConnectionException) {
                                ClientConnectionException failure = (ClientConnectionException)err;

                                int attemptsLimit = getRetryLimit() - 1;

                                if (attemptsLimit == 0 || !shouldRetry(op, 0, failure)) {
                                    fut.completeExceptionally(err);
                                    return null;
                                }

                                handleServiceAsync(fut, op, payloadWriter, payloadReader, attemptsLimit, failure);
                                return null;
                            }

                            fut.completeExceptionally(err);
                            return null;
                        }));

                if (result != null)
                    return new IgniteClientFutureImpl<>(fut);
            }

        }

        return serviceAsync(op, payloadWriter, payloadReader);
    }

    /**
     * @param cacheName Cache name.
     */
    public void registerCacheIfCustomAffinity(String cacheName) {
        ClientPartitionAwarenessMapperFactory factory = clientCfg.getPartitionAwarenessMapperFactory();

        if (factory == null)
            return;

        affinityCtx.registerCache(cacheName);
    }

    /**
     * @param cacheName Cache name.
     */
    public void unregisterCacheIfCustomAffinity(String cacheName) {
        affinityCtx.unregisterCache(cacheName);
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

                        Boolean result = applyOnNodeChannel(nodeId, channel ->
                            channel.service(ClientOperation.CACHE_PARTITIONS,
                                affinityCtx::writePartitionsUpdateRequest,
                                affinityCtx::readPartitionsUpdateResponse)
                        );

                        if (result != null)
                            return result;
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
     * @return List of host:port_range address lines parsed as {@link InetSocketAddress}.
     */
    private static List<InetSocketAddress> parsedAddresses(String[] addrs) throws ClientException {
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
                .map(p -> InetSocketAddress.createUnresolved(r.host(), p))
            ).collect(Collectors.toList());
    }

    /**
     * Roll current default channel if specified holder equals to it.
     */
    private void rollCurrentChannel(ClientChannelHolder hld) {
        curChannelsGuard.writeLock().lock();

        try {
            int idx = curChIdx;
            List<ClientChannelHolder> holders = channels;

            ClientChannelHolder dfltHld = holders.get(idx);

            if (dfltHld == hld) {
                idx += 1;

                if (idx >= holders.size())
                    curChIdx = 0;
                else
                    curChIdx = idx;
            }
        }
        finally {
            curChannelsGuard.writeLock().unlock();
        }
    }

    /**
     * On current channel failure.
     */
    private void onChannelFailure(ClientChannel ch, Throwable t) {
        // There is nothing wrong if curChIdx was concurrently changed, since channel was closed by another thread
        // when current index was changed and no other wrong channel will be closed by current thread because
        // onChannelFailure checks channel binded to the holder before closing it.
        onChannelFailure(channels.get(curChIdx), ch, t);
    }

    /**
     * On channel of the specified holder failure.
     */
    private void onChannelFailure(ClientChannelHolder hld, ClientChannel ch, Throwable t) {
        log.warning("Channel failure [address=" + hld.chCfg.getAddress() + ", err=" + t.getMessage() + ']', t);

        if (ch != null && ch == hld.ch)
            hld.closeChannel();

        chFailLsnrs.forEach(Runnable::run);

        // Roll current channel even if a topology changes. To help find working channel faster.
        rollCurrentChannel(hld);

        // For partiton awareness it's already initializing asynchronously in #onTopologyChanged.
        if (addressFinderAddressesChanged() || (scheduledChannelsReinit.get() && !partitionAwarenessEnabled))
            channelsInit();
    }

    /**
     * Checks whether addressFinder returns a different set of addresses.
     */
    private boolean addressFinderAddressesChanged() {
        if (clientCfg.getAddressesFinder() == null)
            return false;

        String[] hostAddrs = clientCfg.getAddressesFinder().getAddresses();
        return !Arrays.equals(hostAddrs, prevHostAddrs);
    }

    /**
     * Asynchronously try to establish a connection to all configured servers.
     */
    private void initAllChannelsAsync() {
        ForkJoinPool.commonPool().submit(
            () -> {
                List<ClientChannelHolder> holders = channels;

                for (ClientChannelHolder hld : holders) {
                    if (closed || (startChannelsReInit > finishChannelsReInit))
                        return; // New reinit task scheduled or channel is closed.

                    try {
                        hld.getOrCreateChannel(true);
                    }
                    catch (Exception e) {
                        log.warning("Failed to initialize channel [address=" + hld.chCfg.getAddress() + ", err=" + e.getMessage() + ']', e);
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
        if (affinityCtx.updateLastTopologyVersion(ch.serverTopologyVersion(), ch.serverNodeId())) {
            if (scheduledChannelsReinit.compareAndSet(false, true)) {
                // If partition awareness is disabled then only schedule and wait for the default channel to fail.
                if (partitionAwarenessEnabled)
                    ForkJoinPool.commonPool().submit(this::channelsInit);
            }
        }
    }

    /**
     * @param chFailLsnr Listener for the channel fail (disconnect).
     */
    public void addChannelFailListener(Runnable chFailLsnr) {
        chFailLsnrs.add(chFailLsnr);
    }

    /**
     * Should the channel initialization be stopped.
     */
    private boolean shouldStopChannelsReinit() {
        return scheduledChannelsReinit.get() || closed;
    }

    /**
     * Init channel holders to all nodes.
     * @return boolean wheter channels was reinited.
     */
    synchronized boolean initChannelHolders() {
        List<ClientChannelHolder> holders = channels;

        startChannelsReInit = System.currentTimeMillis();

        // Enable parallel threads to schedule new init of channel holders.
        scheduledChannelsReinit.set(false);

        List<InetSocketAddress> newAddrs = null;

        if (clientCfg.getAddressesFinder() != null) {
            String[] hostAddrs = clientCfg.getAddressesFinder().getAddresses();

            if (hostAddrs.length == 0)
                throw new ClientException("Empty addresses");

            if (!Arrays.equals(hostAddrs, prevHostAddrs)) {
                newAddrs = parsedAddresses(hostAddrs);
                prevHostAddrs = hostAddrs;
            }
        }
        else if (holders == null)
            newAddrs = parsedAddresses(clientCfg.getAddresses());

        if (newAddrs == null) {
            finishChannelsReInit = System.currentTimeMillis();

            return true;
        }

        Map<InetSocketAddress, ClientChannelHolder> curAddrs = new HashMap<>();

        Map<InetSocketAddress, ClientChannelHolder> newHoldersMap = new HashMap<>();

        Set<InetSocketAddress> newAddrsSet = new HashSet<>(newAddrs);

        // Close obsolete holders or map old but valid addresses to holders
        if (holders != null) {
            for (ClientChannelHolder h : holders) {
                if (newAddrsSet.contains(h.getAddress()))
                    curAddrs.put(h.getAddress(), h);
                else
                    h.close();
            }
        }

        List<ClientChannelHolder> reinitHolders = new ArrayList<>();

        // The variable holds a new index of default channel after topology change.
        // Suppose that reuse of the channel is better than open new connection.
        int dfltChannelIdx = -1;

        ClientChannelHolder currDfltHolder = null;

        int idx = curChIdx;

        if (idx != -1)
            currDfltHolder = holders.get(idx);

        for (InetSocketAddress addr : newAddrs) {
            if (shouldStopChannelsReinit())
                return false;

            // Create new holders for new addrs.
            if (!curAddrs.containsKey(addr)) {
                ClientChannelHolder hld =
                        newHoldersMap.
                                computeIfAbsent(addr,
                                        a -> new ClientChannelHolder(new ClientChannelConfiguration(clientCfg, a)));

                reinitHolders.add(hld);

                continue;
            }

            // This holder is up to date.
            ClientChannelHolder hld = curAddrs.get(addr);

            reinitHolders.add(hld);

            if (hld == currDfltHolder)
                dfltChannelIdx = reinitHolders.size() - 1;
        }

        if (dfltChannelIdx == -1) {
            // If holder is not specified get the random holder from the range of holders with the same port.
            reinitHolders.sort(Comparator.comparingInt(h -> h.getAddress().getPort()));

            int limit = 0;
            int port = reinitHolders.get(0).getAddress().getPort();

            while (limit + 1 < reinitHolders.size() && reinitHolders.get(limit + 1).getAddress().getPort() == port)
                limit++;

            dfltChannelIdx = ThreadLocalRandom.current().nextInt(limit + 1);
        }

        curChannelsGuard.writeLock().lock();

        try {
            channels = reinitHolders;

            curChIdx = dfltChannelIdx;
        }
        finally {
            curChannelsGuard.writeLock().unlock();
        }

        finishChannelsReInit = System.currentTimeMillis();

        return true;
    }

    /**
     * Establishing connections to servers. If partition awareness feature is enabled connections are created
     * for every configured server. Otherwise only default channel is connected.
     */
    void channelsInit() {
        // Do not establish connections if interrupted.
        if (!initChannelHolders())
            return;

        // Apply no-op function. Establish default channel connection.
        applyOnDefaultChannel(channel -> null, null);

        if (partitionAwarenessEnabled)
            initAllChannelsAsync();
    }

    /**
     * Apply specified {@code function} on a channel corresponding to specified {@code nodeId}.
     */
    private <T> T applyOnNodeChannel(UUID nodeId, Function<ClientChannel, T> function) {
        ClientChannelHolder hld = null;
        ClientChannel channel = null;

        try {
            hld = nodeChannels.get(nodeId);

            channel = hld != null ? hld.getOrCreateChannel() : null;

            if (channel != null)
                return function.apply(channel);
        }
        catch (ClientConnectionException e) {
            onChannelFailure(hld, channel, e);
        }

        return null;
    }

    /** */
    <T> T applyOnDefaultChannel(Function<ClientChannel, T> function, ClientOperation op) {
        return applyOnDefaultChannel(function, op, getRetryLimit(), DO_NOTHING);
    }

    /**
     * Apply specified {@code function} on any of available channel.
     */
    private <T> T applyOnDefaultChannel(Function<ClientChannel, T> function,
                                        ClientOperation op,
                                        int attemptsLimit,
                                        Consumer<Integer> attemptsCallback) {
        ClientConnectionException failure = null;

        for (int attempt = 0; attempt < attemptsLimit; attempt++) {
            ClientChannelHolder hld = null;
            ClientChannel c = null;

            try {
                if (closed)
                    throw new ClientException("Channel is closed");

                curChannelsGuard.readLock().lock();

                try {
                    if (!partitionAwarenessEnabled || channelsCnt.get() <= 1 || attempt != 0)
                        hld = channels.get(curChIdx);
                    else {
                        // Make first attempt with the random open channel.
                        int idx = ThreadLocalRandom.current().nextInt(channels.size());
                        int idx0 = idx;

                        do {
                            hld = channels.get(idx);

                            if (++idx == channels.size())
                                idx = 0;
                        }
                        while (hld.ch == null && idx != idx0);
                    }
                }
                finally {
                    curChannelsGuard.readLock().unlock();
                }

                c = hld.getOrCreateChannel();

                if (c != null) {
                    attemptsCallback.accept(attempt + 1);

                    return function.apply(c);
                }
            }
            catch (ClientConnectionException e) {
                if (failure == null)
                    failure = e;
                else
                    failure.addSuppressed(e);

                onChannelFailure(hld, c, e);

                if (op != null && !shouldRetry(op, attempt, e))
                    break;
            }
        }

        throw failure;
    }

    /**
     * Try apply specified {@code function} on a channel corresponding to {@code tryNodeId}.
     * If failed then apply the function on any available channel.
     */
    private <T> T applyOnNodeChannelWithFallback(UUID tryNodeId, Function<ClientChannel, T> function, ClientOperation op) {
        ClientChannelHolder hld = nodeChannels.get(tryNodeId);

        int retryLimit = getRetryLimit();

        if (hld != null) {
            ClientChannel channel = null;

            try {
                channel = hld.getOrCreateChannel();

                if (channel != null)
                    return function.apply(channel);

            }
            catch (ClientConnectionException e) {
                onChannelFailure(hld, channel, e);

                retryLimit -= 1;

                if (retryLimit == 0 || !shouldRetry(op, 0, e))
                    throw e;
            }
        }

        return applyOnDefaultChannel(function, op, retryLimit, DO_NOTHING);
    }

    /** Get retry limit. */
    private int getRetryLimit() {
        List<ClientChannelHolder> holders = channels;

        if (holders == null)
            throw new ClientException("Connections to nodes aren't initialized.");

        int size = holders.size();

        return clientCfg.getRetryLimit() > 0 ? Math.min(clientCfg.getRetryLimit(), size) : size;
    }

    /** Determines whether specified operation should be retried. */
    private boolean shouldRetry(ClientOperation op, int iteration, ClientConnectionException exception) {
        ClientOperationType opType = op.toPublicOperationType();

        if (opType == null) {
            if (log.isDebugEnabled())
                log.debug("Retrying system operation [op=" + op + ", iteration=" + iteration + ']');

            return true; // System operation.
        }

        ClientRetryPolicy plc = clientCfg.getRetryPolicy();

        if (plc == null)
            return false;

        ClientRetryPolicyContext ctx = new ClientRetryPolicyContextImpl(clientCfg, opType, iteration, exception);

        try {
            boolean res = plc.shouldRetry(ctx);

            if (log.isDebugEnabled())
                log.debug("Retry policy returned " + res + " [op=" + op + ", iteration=" + iteration + ']');

            return res;
        }
        catch (Throwable t) {
            exception.addSuppressed(t);
            return false;
        }
    }

    /**
     * @return Affinity context.
     */
    ClientCacheAffinityContext affinityContext() {
        return affinityCtx;
    }

    /**
     * Channels holder.
     */
    @SuppressWarnings("PackageVisibleInnerClass") // Visible for tests.
    class ClientChannelHolder {
        /** Channel configuration. */
        private final ClientChannelConfiguration chCfg;

        /** Channel. */
        private volatile ClientChannel ch;

        /** ID of the last server node that {@link ch} is or was connected to. */
        private volatile UUID serverNodeId;

        /** Address that holder is bind to (chCfg.addr) is not in use now. So close the holder. */
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

                    ClientChannel channel = chFactory.apply(chCfg, connMgr);

                    if (channel.serverNodeId() != null) {
                        channel.addTopologyChangeListener(ReliableChannel.this::onTopologyChanged);

                        UUID prevId = serverNodeId;

                        if (prevId != null && !prevId.equals(channel.serverNodeId()))
                            nodeChannels.remove(prevId, this);

                        if (!channel.serverNodeId().equals(prevId)) {
                            serverNodeId = channel.serverNodeId();

                            // There could be multiple holders map to the same serverNodeId if user provide the same
                            // address multiple times in configuration.
                            nodeChannels.putIfAbsent(channel.serverNodeId(), this);
                        }
                    }

                    ch = channel;

                    channelsCnt.incrementAndGet();
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

                ch = null;

                channelsCnt.decrementAndGet();
            }
        }

        /**
         * Close holder.
         */
        void close() {
            close = true;

            if (serverNodeId != null)
                nodeChannels.remove(serverNodeId, this);

            closeChannel();
        }

        /**
         * Wheteher the holder is closed. For test purposes.
         */
        boolean isClosed() {
            return close;
        }

        /**
         * Get address of the channel. For test purposes.
         */
        InetSocketAddress getAddress() {
            return chCfg.getAddress();
        }
    }

    /**
     * Get holders reference. For test purposes.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") // For tests.
    List<ClientChannelHolder> getChannelHolders() {
        return channels;
    }

    /**
     * Get node channels reference. For test purposes.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") // For tests.
    Map<UUID, ClientChannelHolder> getNodeChannels() {
        return nodeChannels;
    }

    /**
     * Get index of current (default) channel holder. For test purposes.
     */
    int getCurrentChannelIndex() {
        return curChIdx;
    }

    /**
     * Get scheduledChannelsReinit reference. For test purposes.
     */
    AtomicBoolean getScheduledChannelsReinit() {
        return scheduledChannelsReinit;
    }
}
