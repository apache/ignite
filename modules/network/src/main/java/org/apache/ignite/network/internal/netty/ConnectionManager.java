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

package org.apache.ignite.network.internal.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.NetworkMessage;
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

    /** Channels. */
    private final Map<SocketAddress, NettySender> channels = new ConcurrentHashMap<>();

    /** Clients. */
    private final Map<SocketAddress, NettyClient> clients = new ConcurrentHashMap<>();

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /** Message listeners. */
    private final List<BiConsumer<SocketAddress, NetworkMessage>> listeners = new CopyOnWriteArrayList<>();

    /**
     * Constructor.
     *
     * @param port Server port.
     * @param registry Serialization registry.
     */
    public ConnectionManager(int port, MessageSerializationRegistry registry) {
        this.serializationRegistry = registry;
        this.server = new NettyServer(port, this::onNewIncomingChannel, this::onMessage, serializationRegistry);
        this.clientBootstrap = createClientBootstrap(clientWorkerGroup, serializationRegistry, this::onMessage);
    }

    /**
     * Starts the server.
     *
     * @throws IgniteInternalException If failed to start.
     */
    public void start() throws IgniteInternalException {
        try {
            server.start().join();
        }
        catch (CompletionException e) {
            Throwable cause = e.getCause();
            throw new IgniteInternalException("Failed to start server: " + cause.getMessage(), cause);
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
     * @param address Another node's address.
     * @return Sender.
     */
    public CompletableFuture<NettySender> channel(SocketAddress address) {
        NettySender channel = channels.compute(
            address,
            (addr, sender) -> (sender == null || !sender.isOpen()) ? null : sender
        );

        if (channel != null)
            return CompletableFuture.completedFuture(channel);

        NettyClient client = clients.compute(address, (addr, existingClient) ->
            existingClient != null && !existingClient.failedToConnect() && !existingClient.isDisconnected() ?
                existingClient : connect(addr)
        );

        return client.sender();
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
        SocketAddress remoteAddress = channel.remoteAddress();
        channels.put(remoteAddress, channel);
    }

    /**
     * Create new client from this node to specified address.
     *
     * @param address Target address.
     * @return New netty client.
     */
    private NettyClient connect(SocketAddress address) {
        NettyClient client = new NettyClient(address, serializationRegistry);

        client.start(clientBootstrap).whenComplete((sender, throwable) -> {
            if (throwable != null)
                clients.remove(address);
            else
                channels.put(address, sender);
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
             LOG.warn("Failed to stop the ConnectionManager: " + e.getMessage());
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
     * @return Collection of all the clients started by this connection manager.
     */
    @TestOnly
    public Collection<NettyClient> clients() {
        return Collections.unmodifiableCollection(clients.values());
    }

    /**
     * Creates a {@link Bootstrap} for clients, providing channel handlers and options.
     *
     * @param eventLoopGroup Event loop group for channel handling.
     * @param serializationRegistry Serialization registry.
     * @param messageListener Message listener.
     * @return Bootstrap for clients.
     */
    public static Bootstrap createClientBootstrap(
        EventLoopGroup eventLoopGroup,
        MessageSerializationRegistry serializationRegistry,
        BiConsumer<SocketAddress, NetworkMessage> messageListener
    ) {
        Bootstrap clientBootstrap = new Bootstrap();

        clientBootstrap.group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            // See NettyServer#start for netty configuration details.
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(new ChannelInitializer<SocketChannel>() {
                /** {@inheritDoc} */
                @Override public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(
                        new InboundDecoder(serializationRegistry),
                        new MessageHandler(messageListener),
                        new ChunkedWriteHandler()
                    );
                }
            });

        return clientBootstrap;
    }
}
