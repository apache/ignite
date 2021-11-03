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

package org.apache.ignite.internal.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.IgniteClientAuthenticationException;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.io.netty.NettyClientConnectionMultiplexer;

/**
 * Communication channel with failover and partition awareness.
 */
public final class ReliableChannel implements AutoCloseable {
    /** Do nothing helper function. */
    private static final Consumer<Integer> DO_NOTHING = (v) -> {
    };

    /** Channel factory. */
    private final BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory;

    /** Client channel holders for each configured address. */
    private volatile List<ClientChannelHolder> channels;

    /** Index of the current channel. */
    private volatile int curChIdx = -1;

    /** Client configuration. */
    private final IgniteClientConfiguration clientCfg;

    /** Node channels. */
    private final Map<UUID, ClientChannelHolder> nodeChannels = new ConcurrentHashMap<>();

    /** Channels reinit was scheduled. */
    private final AtomicBoolean scheduledChannelsReinit = new AtomicBoolean();

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

    /**
     * Constructor.
     *
     * @param chFactory Channel factory.
     * @param clientCfg Client config.
     */
    ReliableChannel(BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory,
            IgniteClientConfiguration clientCfg) {
        if (chFactory == null) {
            throw new NullPointerException("chFactory");
        }

        if (clientCfg == null) {
            throw new NullPointerException("clientCfg");
        }

        this.clientCfg = clientCfg;
        this.chFactory = chFactory;

        connMgr = new NettyClientConnectionMultiplexer();
        connMgr.start(clientCfg);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void close() {
        closed = true;

        connMgr.stop();

        List<ClientChannelHolder> holders = channels;

        if (holders != null) {
            for (ClientChannelHolder hld : holders) {
                hld.close();
            }
        }
    }

    /**
     * Sends request and handles response asynchronously.
     *
     * @param opCode        Operation code.
     * @param payloadWriter Payload writer.
     * @param payloadReader Payload reader.
     * @param <T>           response type.
     * @return Future for the operation.
     */
    public <T> CompletableFuture<T> serviceAsync(
            int opCode,
            PayloadWriter payloadWriter,
            PayloadReader<T> payloadReader
    ) {
        CompletableFuture<T> fut = new CompletableFuture<>();

        // Use the only one attempt to avoid blocking async method.
        handleServiceAsync(fut, opCode, payloadWriter, payloadReader, 1, null);

        return fut;
    }

    /**
     * Sends request without payload and handles response asynchronously.
     *
     * @param opCode        Operation code.
     * @param payloadReader Payload reader.
     * @param <T>           Response type.
     * @return Future for the operation.
     */
    public <T> CompletableFuture<T> serviceAsync(int opCode, PayloadReader<T> payloadReader) {
        return serviceAsync(opCode, null, payloadReader);
    }

    private <T> void handleServiceAsync(final CompletableFuture<T> fut,
            int opCode,
            PayloadWriter payloadWriter,
            PayloadReader<T> payloadReader,
            int attemptsLimit,
            IgniteClientConnectionException failure) {
        ClientChannel ch;
        // Workaround to store used attempts value within lambda body.
        var attemptsCnt = new int[1];

        try {
            ch = applyOnDefaultChannel(channel -> channel, attemptsLimit, v -> attemptsCnt[0] = v);
        } catch (Throwable ex) {
            if (failure != null) {
                failure.addSuppressed(ex);

                fut.completeExceptionally(failure);

                return;
            }

            fut.completeExceptionally(ex);

            return;
        }

        ch
                .serviceAsync(opCode, payloadWriter, payloadReader)
                .handle((res, err) -> {
                    if (err == null) {
                        fut.complete(res);

                        return null;
                    }

                    IgniteClientConnectionException failure0 = failure;

                    if (err instanceof IgniteClientConnectionException) {
                        try {
                            // Will try to reinit channels if topology changed.
                            onChannelFailure(ch);
                        } catch (Throwable ex) {
                            fut.completeExceptionally(ex);

                            return null;
                        }

                        if (failure0 == null) {
                            failure0 = (IgniteClientConnectionException) err;
                        } else {
                            failure0.addSuppressed(err);
                        }

                        int leftAttempts = attemptsLimit - attemptsCnt[0];

                        // If it is a first retry then reset attempts (as for initialization we use only 1 attempt).
                        if (failure == null) {
                            leftAttempts = getRetryLimit() - 1;
                        }

                        if (leftAttempts > 0) {
                            handleServiceAsync(fut, opCode, payloadWriter, payloadReader, leftAttempts, failure0);

                            return null;
                        }
                    } else {
                        fut.completeExceptionally(err instanceof IgniteClientException
                                ? err
                                : new IgniteClientException(err.getMessage(), err));

                        return null;
                    }

                    fut.completeExceptionally(failure0);

                    return null;
                });
    }

    /**
     * Sends request with payload and handles response asynchronously.
     *
     * @param opCode        Operation code.
     * @param payloadWriter Payload writer.
     * @return Future for the operation.
     */
    public CompletableFuture<Void> requestAsync(int opCode, PayloadWriter payloadWriter) {
        return serviceAsync(opCode, payloadWriter, null);
    }

    /**
     * @return host:port_range address lines parsed as {@link InetSocketAddress} as a key. Value is the amount of appearences of an address
     *      in {@code addrs} parameter.
     */
    private static Map<InetSocketAddress, Integer> parsedAddresses(String[] addrs) throws IgniteClientException {
        if (addrs == null || addrs.length == 0) {
            throw new IgniteClientException("Empty addresses");
        }

        Collection<HostAndPortRange> ranges = new ArrayList<>(addrs.length);

        for (String a : addrs) {
            ranges.add(HostAndPortRange.parse(
                    a,
                    IgniteClientConfiguration.DFLT_PORT,
                    IgniteClientConfiguration.DFLT_PORT + IgniteClientConfiguration.DFLT_PORT_RANGE,
                    "Failed to parse Ignite server address"
            ));
        }

        return ranges.stream()
                .flatMap(r -> IntStream
                        .rangeClosed(r.portFrom(), r.portTo()).boxed()
                        .map(p -> InetSocketAddress.createUnresolved(r.host(), p))
                )
                .collect(Collectors.toMap(a -> a, a -> 1, Integer::sum));
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

                if (idx >= holders.size()) {
                    curChIdx = 0;
                } else {
                    curChIdx = idx;
                }
            }
        } finally {
            curChannelsGuard.writeLock().unlock();
        }
    }

    /**
     * On current channel failure.
     */
    private void onChannelFailure(ClientChannel ch) {
        // There is nothing wrong if curChIdx was concurrently changed, since channel was closed by another thread
        // when current index was changed and no other wrong channel will be closed by current thread because
        // onChannelFailure checks channel binded to the holder before closing it.
        onChannelFailure(channels.get(curChIdx), ch);
    }

    /**
     * On channel of the specified holder failure.
     */
    private void onChannelFailure(ClientChannelHolder hld, ClientChannel ch) {
        if (ch != null && ch == hld.ch) {
            hld.closeChannel();
        }

        chFailLsnrs.forEach(Runnable::run);

        // Roll current channel even if a topology changes. To help find working channel faster.
        rollCurrentChannel(hld);

        if (scheduledChannelsReinit.get()) {
            channelsInitAsync();
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
     *
     * @return boolean wheter channels was reinited.
     */
    synchronized boolean initChannelHolders() {
        List<ClientChannelHolder> holders = channels;

        // Enable parallel threads to schedule new init of channel holders.
        scheduledChannelsReinit.set(false);

        Map<InetSocketAddress, Integer> newAddrs = null;

        if (clientCfg.addressesFinder() != null) {
            String[] hostAddrs = clientCfg.addressesFinder().getAddresses();

            if (hostAddrs.length == 0) {
                throw new IgniteClientException("Empty addresses");
            }

            if (!Arrays.equals(hostAddrs, prevHostAddrs)) {
                newAddrs = parsedAddresses(hostAddrs);
                prevHostAddrs = hostAddrs;
            }
        } else if (holders == null) {
            newAddrs = parsedAddresses(clientCfg.addresses());
        }

        if (newAddrs == null) {
            return true;
        }

        Map<InetSocketAddress, ClientChannelHolder> curAddrs = new HashMap<>();
        Set<InetSocketAddress> allAddrs = new HashSet<>(newAddrs.keySet());

        if (holders != null) {
            for (int i = 0; i < holders.size(); i++) {
                ClientChannelHolder h = holders.get(i);

                curAddrs.put(h.chCfg.getAddress(), h);
                allAddrs.add(h.chCfg.getAddress());
            }
        }

        List<ClientChannelHolder> reinitHolders = new ArrayList<>();

        // The variable holds a new index of default channel after topology change.
        // Suppose that reuse of the channel is better than open new connection.
        int dfltChannelIdx = -1;

        ClientChannelHolder currDfltHolder = null;

        int idx = curChIdx;

        if (idx != -1) {
            currDfltHolder = holders.get(idx);
        }

        for (InetSocketAddress addr : allAddrs) {
            if (shouldStopChannelsReinit()) {
                return false;
            }

            // Obsolete addr, to be removed.
            if (!newAddrs.containsKey(addr)) {
                curAddrs.get(addr).close();

                continue;
            }

            // Create new holders for new addrs.
            if (!curAddrs.containsKey(addr)) {
                ClientChannelHolder hld = new ClientChannelHolder(new ClientChannelConfiguration(clientCfg, addr));

                for (int i = 0; i < newAddrs.get(addr); i++) {
                    reinitHolders.add(hld);
                }

                continue;
            }

            // This holder is up to date.
            ClientChannelHolder hld = curAddrs.get(addr);

            for (int i = 0; i < newAddrs.get(addr); i++) {
                reinitHolders.add(hld);
            }

            if (hld == currDfltHolder) {
                dfltChannelIdx = reinitHolders.size() - 1;
            }
        }

        if (dfltChannelIdx == -1) {
            dfltChannelIdx = new Random().nextInt(reinitHolders.size());
        }

        curChannelsGuard.writeLock().lock();

        try {
            channels = reinitHolders;
            curChIdx = dfltChannelIdx;
        } finally {
            curChannelsGuard.writeLock().unlock();
        }

        return true;
    }

    /**
     * Establishing connections to servers. If partition awareness feature is enabled connections are created for every configured server.
     * Otherwise, only default channel is connected.
     */
    CompletableFuture<Void> channelsInitAsync() {
        // Do not establish connections if interrupted.
        if (!initChannelHolders()) {
            return CompletableFuture.completedFuture(null);
        }

        // Apply no-op function. Establish default channel connection.
        applyOnDefaultChannel(channel -> null);

        // TODO: Async startup IGNITE-15357.
        return CompletableFuture.completedFuture(null);
    }

    /**
     *
     */
    private <T> T applyOnDefaultChannel(Function<ClientChannel, T> function) {
        return applyOnDefaultChannel(function, getRetryLimit(), DO_NOTHING);
    }

    /**
     * Apply specified {@code function} on any of available channel.
     */
    private <T> T applyOnDefaultChannel(Function<ClientChannel, T> function,
            int attemptsLimit,
            Consumer<Integer> attemptsCallback) {
        Throwable failure = null;

        for (int attempt = 0; attempt < attemptsLimit; attempt++) {
            ClientChannelHolder hld = null;
            ClientChannel c = null;

            try {
                if (closed) {
                    throw new IgniteClientException("Channel is closed");
                }

                curChannelsGuard.readLock().lock();

                try {
                    hld = channels.get(curChIdx);
                } finally {
                    curChannelsGuard.readLock().unlock();
                }

                c = hld.getOrCreateChannel();

                if (c != null) {
                    attemptsCallback.accept(attempt + 1);

                    return function.apply(c);
                }
            } catch (Throwable e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }

                onChannelFailure(hld, c);
            }
        }

        throw new IgniteClientConnectionException("Failed to connect", failure);
    }

    /** Get retry limit. */
    private int getRetryLimit() {
        List<ClientChannelHolder> holders = channels;

        if (holders == null) {
            throw new IgniteClientException("Connections to nodes aren't initialized.");
        }

        int size = holders.size();

        return clientCfg.retryLimit() > 0 ? Math.min(clientCfg.retryLimit(), size) : size;
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

        /** ID of the last server node that channel is or was connected to. */
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

            reconnectRetries = chCfg.clientConfiguration().reconnectThrottlingRetries() > 0
                    && chCfg.clientConfiguration().reconnectThrottlingPeriod() > 0L
                    ? new long[chCfg.clientConfiguration().reconnectThrottlingRetries()]
                    : null;
        }

        /**
         * @return Whether reconnect throttling should be applied.
         */
        private boolean applyReconnectionThrottling() {
            if (reconnectRetries == null) {
                return false;
            }

            long ts = System.currentTimeMillis();

            for (int i = 0; i < reconnectRetries.length; i++) {
                if (ts - reconnectRetries[i] >= chCfg.clientConfiguration().reconnectThrottlingPeriod()) {
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
                throws IgniteClientConnectionException, IgniteClientAuthenticationException {
            return getOrCreateChannel(false);
        }

        /**
         * Get or create channel.
         */
        private ClientChannel getOrCreateChannel(boolean ignoreThrottling)
                throws IgniteClientConnectionException, IgniteClientAuthenticationException {
            if (ch == null && !close) {
                synchronized (this) {
                    if (close) {
                        return null;
                    }

                    if (ch != null) {
                        return ch;
                    }

                    if (!ignoreThrottling && applyReconnectionThrottling()) {
                        throw new IgniteClientConnectionException("Reconnect is not allowed due to applied throttling");
                    }

                    ch = chFactory.apply(chCfg, connMgr);
                }
            }

            return ch;
        }

        /**
         * Close channel.
         */
        private synchronized void closeChannel() {
            if (ch != null) {
                try {
                    ch.close();
                } catch (Exception ignored) {
                    // No op.
                }

                ch = null;
            }
        }

        /**
         * Close holder.
         */
        void close() {
            close = true;

            if (serverNodeId != null) {
                nodeChannels.remove(serverNodeId, this);
            }

            closeChannel();
        }
    }
}
