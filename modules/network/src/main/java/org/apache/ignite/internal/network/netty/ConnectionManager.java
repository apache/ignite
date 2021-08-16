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

package org.apache.ignite.internal.network.netty;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Class that manages connections both incoming and outgoing.
 */
public class ConnectionManager {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ConnectionManager.class);

    /** Latest version of the direct marshalling protocol. */
    public static final byte DIRECT_PROTOCOL_VERSION = 1;

    /** Client bootstrap. */
    private final Bootstrap clientBootstrap;

    /** Client socket channel handler event loop group. */
    private final EventLoopGroup clientWorkerGroup = new NioEventLoopGroup();

    /** Server. */
    private final NettyServer server;

    /** Channels map from consistentId to {@link NettySender}. */
    private final Map<String, NettySender> channels = new ConcurrentHashMap<>();

    /** Clients. */
    private final Map<SocketAddress, NettyClient> clients = new ConcurrentHashMap<>();

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /** Message listeners. */
    private final List<BiConsumer<SocketAddress, NetworkMessage>> listeners = new CopyOnWriteArrayList<>();

    /** Node consistent id. */
    private final String consistentId;

    /** Client handshake manager factory. */
    private final Supplier<HandshakeManager> clientHandshakeManagerFactory;

    /** Start flag. */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /** Stop flag. */
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    /**
     * Constructor.
     *
     * @param port Server port.
     * @param registry Serialization registry.
     * @param consistentId Consistent id of this node.
     * @param serverHandshakeManagerFactory Server handshake manager factory.
     * @param clientHandshakeManagerFactory Client handshake manager factory.
     */
    public ConnectionManager(
        int port,
        MessageSerializationRegistry registry,
        String consistentId,
        Supplier<HandshakeManager> serverHandshakeManagerFactory,
        Supplier<HandshakeManager> clientHandshakeManagerFactory
    ) {
        this.serializationRegistry = registry;
        this.consistentId = consistentId;
        this.clientHandshakeManagerFactory = clientHandshakeManagerFactory;
        this.server = new NettyServer(
            port,
            serverHandshakeManagerFactory,
            this::onNewIncomingChannel,
            this::onMessage,
            serializationRegistry
        );
        this.clientBootstrap = createClientBootstrap(clientWorkerGroup, serializationRegistry);
    }

    /**
     * Starts the server.
     *
     * @throws IgniteInternalException If failed to start.
     */
    public void start() throws IgniteInternalException {
        try {
            boolean wasStarted = started.getAndSet(true);

            if (wasStarted)
                throw new IgniteInternalException("Attempted to start an already started connection manager");

            if (stopped.get())
                throw new IgniteInternalException("Attempted to start an already stopped connection manager");

            //TODO: timeout value should be extracted into common configuration
            // https://issues.apache.org/jira/browse/IGNITE-14538
            server.start().get(3, TimeUnit.SECONDS);

            LOG.info("Connection created [address=" + server.address() + ']');
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new IgniteInternalException("Failed to start the connection manager: " + cause.getMessage(), cause);
        }
        catch (TimeoutException e) {
            throw new IgniteInternalException("Timeout while waiting for the connection manager to start", e);
        }
        catch (InterruptedException e) {
            throw new IgniteInternalException("Interrupted while starting the connection manager", e);
        }
    }

    /**
     * @return Server local address.
     */
    public SocketAddress getLocalAddress() {
        return server.address();
    }

    /**
     * Gets a {@link NettySender}, that sends data from this node to another node with the specified address.
     * @param consistentId Another node's consistent id.
     * @param address Another node's address.
     * @return Sender.
     */
    public CompletableFuture<NettySender> channel(@Nullable String consistentId, SocketAddress address) {
        if (consistentId != null) {
            // If consistent id is known, try looking up a channel by consistent id. There can be an outbound connection
            // or an inbound connection associated with that consistent id.
            NettySender channel = channels.compute(
                consistentId,
                (addr, sender) -> (sender == null || !sender.isOpen()) ? null : sender
            );

            if (channel != null)
                return CompletableFuture.completedFuture(channel);
        }

        // Get an existing client or create a new one. NettyClient provides a CompletableFuture that resolves
        // when the client is ready for write operations, so previously started client, that didn't establish connection
        // or didn't perform the handhsake operaton, can be reused.
        NettyClient client = clients.compute(address, (addr, existingClient) ->
            existingClient != null && !existingClient.failedToConnect() && !existingClient.isDisconnected() ?
                existingClient : connect(addr)
        );

        CompletableFuture<NettySender> sender = client.sender();

        assert sender != null;

        return sender;
    }

    /**
     * Callback that is called upon receiving a new message.
     *
     * @param from Source of the message.
     * @param message New message.
     */
    private void onMessage(SocketAddress from, NetworkMessage message) {
        listeners.forEach(consumer -> consumer.accept(from, message));
    }

    /**
     * Callback that is called upon new client connected to the server.
     *
     * @param channel Channel from client to this {@link #server}.
     */
    private void onNewIncomingChannel(NettySender channel) {
        channels.put(channel.consistentId(), channel);
    }

    /**
     * Create new client from this node to specified address.
     *
     * @param address Target address.
     * @return New netty client.
     */
    private NettyClient connect(SocketAddress address) {
        var client = new NettyClient(
            address,
            serializationRegistry,
            clientHandshakeManagerFactory.get(),
            this::onMessage
        );

        client.start(clientBootstrap).whenComplete((sender, throwable) -> {
            if (throwable == null)
                channels.put(sender.consistentId(), sender);
            else
                clients.remove(address);
        });

        return client;
    }

    /**
     * Add incoming message listener.
     *
     * @param listener Message listener.
     */
    public void addListener(BiConsumer<SocketAddress, NetworkMessage> listener) {
        listeners.add(listener);
    }

    /**
     * Stops the server and all clients.
     */
    public void stop() {
        boolean wasStopped = this.stopped.getAndSet(true);

        if (wasStopped)
            return;

        Stream<CompletableFuture<Void>> stream = Stream.concat(
            clients.values().stream().map(NettyClient::stop),
            Stream.of(server.stop())
        );

        CompletableFuture<Void> stopFut = CompletableFuture.allOf(stream.toArray(CompletableFuture<?>[]::new));

        try {
            stopFut.join();
            // TODO: IGNITE-14538 quietPeriod and timeout should be configurable.
            clientWorkerGroup.shutdownGracefully(0L, 15, TimeUnit.SECONDS).sync();
        }
        catch (Exception e) {
            LOG.warn("Failed to stop the ConnectionManager: {}", e.getMessage());
        }
    }

    /**
     * @return Connection manager's {@link #server}.
     */
    @TestOnly
    public NettyServer server() {
        return server;
    }

    /**
     * @return This node's consistent id.
     */
    @TestOnly
    public String consistentId() {
        return consistentId;
    }

    /**
     * @return Collection of all the clients started by this connection manager.
     */
    @TestOnly
    public Collection<NettyClient> clients() {
        return Collections.unmodifiableCollection(clients.values());
    }


    /**
     * @return Map of the channels.
     */
    @TestOnly
    public Map<String, NettySender> channels() {
        return Collections.unmodifiableMap(channels);
    }

    /**
     * Creates a {@link Bootstrap} for clients, providing channel handlers and options.
     *
     * @param eventLoopGroup Event loop group for channel handling.
     * @param serializationRegistry Serialization registry.
     * @return Bootstrap for clients.
     */
    public static Bootstrap createClientBootstrap(
        EventLoopGroup eventLoopGroup,
        MessageSerializationRegistry serializationRegistry
    ) {
        Bootstrap clientBootstrap = new Bootstrap();

        clientBootstrap.group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            // See NettyServer#start for netty configuration details.
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_LINGER, 0)
            .option(ChannelOption.TCP_NODELAY, true);

        return clientBootstrap;
    }
}
