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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
import org.apache.ignite.IgniteBinary;
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
import org.apache.ignite.internal.client.thin.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.thin.io.gridnioserver.GridNioClientConnectionMultiplexer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Communication channel with failover and partition awareness.
 */
final class ReliableChannel implements AutoCloseable {
    /** Channel factory. */
    private final BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory;

    /** Client channel holders for each configured address. */
    private volatile List<ClientChannelHolder> channels;

    /** Limit of channels to execute each {@link #service}. */
    private volatile int srvcChannelsLimit;

    /** Index of the current channel. */
    private volatile int curChIdx = -1;

    /** Partition awareness enabled. */
    final boolean partitionAwarenessEnabled;

    /** Cache partition awareness context. */
    private final ClientCacheAffinityContext affinityCtx;

    /** Nodes discovery context. */
    private final ClientDiscoveryContext discoveryCtx;

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

        affinityCtx = new ClientCacheAffinityContext(
            binary,
            clientCfg.getPartitionAwarenessMapperFactory(),
            this::isConnectionEstablished
        );

        discoveryCtx = new ClientDiscoveryContext(clientCfg);

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
        return service(op, payloadWriter, payloadReader, Collections.emptyList());
    }

    /**
     * Send request to one of the passed nodes and handle response.
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
        Function<PayloadInputChannel, T> payloadReader,
        List<UUID> targetNodes
    ) throws ClientException, ClientError {
        if (F.isEmpty(targetNodes))
            return applyOnDefaultChannel(channel -> channel.service(op, payloadWriter, payloadReader), op);

        return applyOnNodeChannelWithFallback(
            targetNodes.get(ThreadLocalRandom.current().nextInt(targetNodes.size())),
            channel -> channel.service(op, payloadWriter, payloadReader),
            op
        );
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
        handleServiceAsync(fut, op, payloadWriter, payloadReader, new ArrayList<>());

        return new IgniteClientFutureImpl<>(fut);
    }

    /**
     * Handles serviceAsync results and retries as needed.
     */
    private <T> void handleServiceAsync(
        final CompletableFuture<T> fut,
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader,
        List<ClientConnectionException> failures
    ) {
        try {
            ClientChannel ch = applyOnDefaultChannel(channel -> channel, null, failures);

            CompletableFuture<T> chFut = applyOnDefaultChannel(
                channel -> applyOnClientChannelAsync(ch, op, payloadWriter, payloadReader),
                null,
                failures
            );

            // Retry use same channel in case of connection exception.
            CompletableFuture<T> retryFut = chFut
                .handle((res, err) -> {
                    if (err == null) {
                        fut.complete(res);

                        return res;
                    }

                    if (!(err instanceof ClientConnectionException)) {
                        fut.completeExceptionally(err);

                        throw new RuntimeException(err);
                    }

                    onChannelFailure(ch, err, failures);

                    if (shouldRetry(op, 1, (ClientConnectionException)err))
                        return applyOnClientChannelAsync(ch, op, payloadWriter, payloadReader);

                    fut.completeExceptionally(err);

                    throw new RuntimeException(err);

                }).thenCompose(f -> (CompletableFuture<T>)f);

            // Try other channels in case of failed retry.
            retryFut.whenComplete((res, err) -> {
                if (fut.isDone() || err == null) {
                    fut.complete(res);

                    return;
                }

                if (!(err instanceof ClientConnectionException)) {
                    fut.completeExceptionally(err);

                    return;
                }

                failures.add((ClientConnectionException)err);

                if (failures.size() < srvcChannelsLimit && shouldRetry(op, failures.size() - 1, (ClientConnectionException)err))
                    handleServiceAsync(fut, op, payloadWriter, payloadReader, failures);
                else
                    fut.completeExceptionally(composeException(failures));
            });
        }
        catch (Throwable ex) {
            fut.completeExceptionally(ex);
        }
    }

    /** */
    private <T> CompletableFuture<T> applyOnClientChannelAsync(
        ClientChannel ch,
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) {
        return ch.serviceAsync(op, payloadWriter, payloadReader);
    }

    /** */
    private <T> Object applyOnClientChannelAsync(
        final CompletableFuture<T> fut,
        ClientChannel ch,
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader,
        List<ClientConnectionException> failures
    ) {
        return ch
            .serviceAsync(op, payloadWriter, payloadReader)
            .handle((res, err) -> {
                if (err == null) {
                    fut.complete(res);

                    return null;
                }

                if (err instanceof ClientConnectionException) {
                    ClientConnectionException failure0 = (ClientConnectionException)err;

                    failures.add(failure0);

                    try {
                        // Will try to reinit channels if topology changed.
                        onChannelFailure(ch, err, failures);
                    }
                    catch (Throwable ex) {
                        fut.completeExceptionally(ex);

                        return null;
                    }

                    if (failures.size() < srvcChannelsLimit && shouldRetry(op, failures.size() - 1, failure0)) {
                        handleServiceAsync(fut, op, payloadWriter, payloadReader, failures);

                        return null;
                    }

                    fut.completeExceptionally(composeException(failures));
                }
                else
                    fut.completeExceptionally(err instanceof ClientException ? err : new ClientException(err));

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
                List<ClientConnectionException> failures = new ArrayList<>();

                Object result = applyOnNodeChannel(
                    affNodeId,
                    channel -> applyOnClientChannelAsync(channel, op, payloadWriter, payloadReader),
                    failures
                );

                // TODO: retry the channel.
                
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

                    List<ClientConnectionException> failures = new ArrayList<>();

                    for (UUID nodeId : lastTop.nodes()) {
                        // Abort iterations when topology changed.
                        if (lastTop != affinityCtx.lastTopology())
                            return false;

                        Boolean result = applyOnNodeChannel(nodeId, channel ->
                            channel.service(ClientOperation.CACHE_PARTITIONS,
                                affinityCtx::writePartitionsUpdateRequest,
                                affinityCtx::readPartitionsUpdateResponse),
                            failures
                        );

                        if (result != null) {
                            if (log.isDebugEnabled()) {
                                log.debug("Cache partitions mapping updated [cacheId=" + cacheId +
                                    ", nodeId=" + nodeId + ']');
                            }

                            return result;
                        }
                    }

                    log.warning("Failed to update cache partitions mapping [cacheId=" + cacheId + ']',
                        composeException(failures));

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
    private void onChannelFailure(ClientChannel ch, Throwable t, @Nullable List<ClientConnectionException> failures) {
        // There is nothing wrong if curChIdx was concurrently changed, since channel was closed by another thread
        // when current index was changed and no other wrong channel will be closed by current thread because
        // onChannelFailure checks channel binded to the holder before closing it.
        onChannelFailure(channels.get(curChIdx), ch, t, failures);
    }

    /**
     * On channel of the specified holder failure.
     */
    private void onChannelFailure(
        ClientChannelHolder hld,
        ClientChannel ch,
        Throwable t,
        @Nullable List<ClientConnectionException> failures
    ) {
        log.warning("Channel failure [channel=" + ch + ", err=" + t.getMessage() + ']', t);

        if (ch != null && ch == hld.ch)
            hld.closeChannel();

        chFailLsnrs.forEach(Runnable::run);

        // Roll current channel even if a topology changes. To help find working channel faster.
        rollCurrentChannel(hld);

        if (channelsCnt.get() == 0 && F.size(failures) == srvcChannelsLimit) {
            // All channels have failed.
            discoveryCtx.reset();
            channelsInit(failures);
        }
        else if (scheduledChannelsReinit.get() && !partitionAwarenessEnabled) {
            // For partiton awareness it's already initializing asynchronously in #onTopologyChanged.
            channelsInit(failures);
        }
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
                        log.warning("Failed to initialize channel [addresses=" + hld.getAddresses() + ", err=" +
                            e.getMessage() + ']', e);
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
        if (log.isDebugEnabled())
            log.debug("Topology change detected [ch=" + ch + ", top=" + ch.serverTopologyVersion() + ']');

        if (affinityCtx.updateLastTopologyVersion(ch.serverTopologyVersion(), ch.serverNodeId())) {
            ForkJoinPool.commonPool().submit(() -> {
                try {
                    discoveryCtx.refresh(ch);
                }
                catch (ClientException e) {
                    log.warning("Failed to get nodes endpoints", e);
                }

                if (scheduledChannelsReinit.compareAndSet(false, true)) {
                    // If partition awareness is disabled then only schedule and wait for the default channel to fail.
                    if (partitionAwarenessEnabled)
                        channelsInit();
                }
            });
        }
    }

    /**
     * @param chFailLsnr Listener for the channel fail (disconnect).
     */
    public void addChannelFailListener(Runnable chFailLsnr) {
        chFailLsnrs.add(chFailLsnr);
    }

    /**
     * Init channel holders to all nodes.
     */
    synchronized void initChannelHolders() {
        List<ClientChannelHolder> holders = channels;

        startChannelsReInit = System.currentTimeMillis();

        // Enable parallel threads to schedule new init of channel holders.
        scheduledChannelsReinit.set(false);

        Collection<List<InetSocketAddress>> newAddrs = discoveryCtx.getEndpoints();

        if (newAddrs == null) {
            finishChannelsReInit = System.currentTimeMillis();

            return;
        }

        Map<InetSocketAddress, ClientChannelHolder> curAddrs = new HashMap<>();

        List<ClientChannelHolder> reinitHolders = new ArrayList<>();

        Set<InetSocketAddress> newAddrsSet = newAddrs.stream().flatMap(Collection::stream).collect(Collectors.toSet());

        // Close obsolete holders or map old but valid addresses to holders
        if (holders != null) {
            for (ClientChannelHolder h : holders) {
                boolean found = false;

                for (InetSocketAddress addr : h.getAddresses()) {
                    // If new endpoints contain at least one of channel addresses, don't close this channel.
                    if (newAddrsSet.contains(addr)) {
                        curAddrs.putIfAbsent(addr, h);

                        found = true;

                        break;
                    }
                }

                // Add connected channels to the list to avoid unnecessary reconnects, unless address finder is used.
                if (clientCfg.getAddressesFinder() == null && h.ch != null && !h.ch.closed())
                    found = true;

                if (!found)
                    h.close();
                else
                    reinitHolders.add(h);
            }
        }

        // The variable holds a new index of default channel after topology change.
        // Suppose that reuse of the channel is better than open new connection.
        int dfltChannelIdx = -1;

        ClientChannelHolder currDfltHolder = null;

        int idx = curChIdx;

        if (idx != -1)
            currDfltHolder = holders.get(idx);

        for (List<InetSocketAddress> addrs : newAddrs) {
            ClientChannelHolder hld = null;

            // Try to find already created channel holder.
            for (InetSocketAddress addr : addrs) {
                hld = curAddrs.get(addr);

                if (hld != null) {
                    if (!hld.getAddresses().equals(addrs)) // Enrich holder addresses.
                        hld.setConfiguration(new ClientChannelConfiguration(clientCfg, addrs));

                    break;
                }
            }

            if (hld == null) { // If not found, create the new one.
                hld = new ClientChannelHolder(new ClientChannelConfiguration(clientCfg, addrs));

                reinitHolders.add(hld);

                for (InetSocketAddress addr : addrs)
                    curAddrs.putIfAbsent(addr, hld);
            }

            if (hld == currDfltHolder)
                dfltChannelIdx = reinitHolders.size() - 1;
        }

        if (dfltChannelIdx == -1) {
            // If holder is not specified get the random holder from the range of holders with the same port.
            reinitHolders.sort(Comparator.comparingInt(h -> F.first(h.getAddresses()).getPort()));

            int limit = 0;
            int port = F.first(reinitHolders.get(0).getAddresses()).getPort();

            while (limit + 1 < reinitHolders.size() && F.first(reinitHolders.get(limit + 1).getAddresses()).getPort() == port)
                limit++;

            dfltChannelIdx = ThreadLocalRandom.current().nextInt(limit + 1);
        }

        curChannelsGuard.writeLock().lock();

        try {
            channels = reinitHolders;

            srvcChannelsLimit = getRetryLimit();

            curChIdx = dfltChannelIdx;
        }
        finally {
            curChannelsGuard.writeLock().unlock();
        }

        finishChannelsReInit = System.currentTimeMillis();
    }

    /**
     * Establishing connections to servers. If partition awareness feature is enabled connections are created
     * for every configured server. Otherwise only default channel is connected.
     */
    void channelsInit() {
        channelsInit(null);
    }

    /**
     * Establishing connections to servers. If partition awareness feature is enabled connections are created
     * for every configured server. Otherwise only default channel is connected.
     */
    void channelsInit(@Nullable List<ClientConnectionException> failures) {
        if (log.isDebugEnabled())
            log.debug("Init channel holders");

        initChannelHolders();

        if (failures == null || failures.size() < srvcChannelsLimit) {
            // Establish default channel connection.
            applyOnDefaultChannel(channel -> null, null, failures);

            if (channelsCnt.get() == 0) {
                // Establish default channel connection and retrive nodes endpoints if applicable.
                boolean discoveryUpdated = applyOnDefaultChannel(discoveryCtx::refresh, null, failures);

                if (discoveryUpdated)
                    initChannelHolders();
            }
        }

        if (partitionAwarenessEnabled)
            initAllChannelsAsync();
    }

    /**
     * Apply specified {@code function} on a channel corresponding to specified {@code nodeId}.
     */
    private <T> T applyOnNodeChannel(
        UUID nodeId,
        Function<ClientChannel, T> function,
        @Nullable List<ClientConnectionException> failures
    ) {
        ClientChannelHolder hld = null;
        ClientChannel channel = null;

        try {
            hld = nodeChannels.get(nodeId);

            channel = hld != null ? hld.getOrCreateChannel() : null;

            if (channel != null)
                return function.apply(channel);
        }
        catch (ClientConnectionException e) {
            if (failures == null)
                failures = new ArrayList<>();

            failures.add(e);

            onChannelFailure(hld, channel, e, failures);
        }

        return null;
    }

    /** */
    <T> T applyOnDefaultChannel(Function<ClientChannel, T> function, ClientOperation op) {
        return applyOnDefaultChannel(function, op, null);
    }

    /**
     * Apply specified {@code function} on any of available channel.
     */
    private <T> T applyOnDefaultChannel(
        Function<ClientChannel, T> function,
        ClientOperation op,
        @Nullable List<ClientConnectionException> failures
    ) {
        int fixedAttemptsLimit = srvcChannelsLimit;

        // An additional attempt is needed because N+1 channels might be used for sending a message - first a random
        // one, then each one from #channels in sequence.
        if (partitionAwarenessEnabled && channelsCnt.get() > 1)
            fixedAttemptsLimit++;

        while (fixedAttemptsLimit > F.size(failures)) {
            ClientChannelHolder hld = null;
            ClientChannel c = null;

            try {
                if (closed)
                    throw new ClientException("Channel is closed");

                curChannelsGuard.readLock().lock();

                try {
                    if (!partitionAwarenessEnabled || channelsCnt.get() <= 1 || F.size(failures) > 0)
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

                ClientChannel c0 = hld.ch;

                c = hld.getOrCreateChannel();

                try {
                    return function.apply(c);
                }
                catch (ClientConnectionException e) {
                    if (c0 == c && shouldRetry(op, F.size(failures), e)) {
                        // In case of stale channel, when partition awareness is enabled, try to reconnect to the
                        // same channel and repeat the operation.
                        onChannelFailure(hld, c, e, failures);

                        c = hld.getOrCreateChannel();

                        return function.apply(c);
                    }
                    else
                        throw e;
                }
            }
            catch (ClientConnectionException e) {
                if (failures == null)
                    failures = new ArrayList<>();

                failures.add(e);

                onChannelFailure(hld, c, e, failures);

                if (op != null && !shouldRetry(op, failures.size() - 1, e))
                    break;
            }
        }

        throw composeException(failures);
    }

    /** */
    private ClientConnectionException composeException(List<ClientConnectionException> failures) {
        if (F.isEmpty(failures))
            return null;

        ClientConnectionException failure = failures.get(0);

        failures.subList(1, failures.size()).forEach(failure::addSuppressed);

        return failure;
    }

    /**
     * Try apply specified {@code function} on a channel corresponding to {@code tryNodeId}.
     * If failed then apply the function on any available channel.
     */
    private <T> T applyOnNodeChannelWithFallback(UUID tryNodeId, Function<ClientChannel, T> function, ClientOperation op) {
        ClientChannelHolder hld = nodeChannels.get(tryNodeId);

        List<ClientConnectionException> failures = null;

        if (hld != null) {
            ClientChannel channel = null;

            try {
                channel = hld.getOrCreateChannel();

                return function.apply(channel);
            }
            catch (ClientConnectionException e) {
                onChannelFailure(hld, channel, e, failures);

                if (!shouldRetry(op, 0, e))
                    throw e;

                try {
                    // In case of stale channel try to reconnect to the same channel and repeat the operation.
                    channel = hld.getOrCreateChannel();

                    return function.apply(channel);
                }

                catch (ClientConnectionException err) {
                    failures = new ArrayList<>();

                    failures.add(err);

                    onChannelFailure(hld, channel, err, failures);

                    if (srvcChannelsLimit == 1 || !shouldRetry(op, 1, e))
                        throw err;
                }
            }
        }

        return applyOnDefaultChannel(function, op, failures);
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

    /** */
    private boolean isConnectionEstablished(UUID node) {
        ClientChannelHolder chHolder = nodeChannels.get(node);

        if (chHolder == null || chHolder.isClosed())
            return false;

        ClientChannel ch = chHolder.ch;

        return ch != null && !ch.closed();
    }

    /**
     * Channels holder.
     */
    @SuppressWarnings("PackageVisibleInnerClass") // Visible for tests.
    class ClientChannelHolder {
        /** Channel configuration. */
        private volatile ClientChannelConfiguration chCfg;

        /** Channel. */
        private volatile ClientChannel ch;

        /** ID of the last server node that {@link #ch} is or was connected to. */
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
            if (close)
                throw new ClientConnectionException("Channel is closed [addresses=" + getAddresses() + ']');

            if (ch == null) {
                synchronized (this) {
                    if (close)
                        throw new ClientConnectionException("Channel is closed [addresses=" + getAddresses() + ']');

                    if (ch != null)
                        return ch;

                    if (!ignoreThrottling && applyReconnectionThrottling())
                        throw new ClientConnectionException("Reconnect is not allowed due to applied throttling" +
                            " [addresses=" + getAddresses() + ']');

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
         * Get addresses of the channel.
         */
        List<InetSocketAddress> getAddresses() {
            return chCfg.getAddresses();
        }

        /**
         * Set new channel configuration.
         */
        void setConfiguration(ClientChannelConfiguration chCfg) {
            this.chCfg = chCfg;
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
