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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.ignite.configuration.schemas.network.InboundView;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Netty server channel wrapper.
 */
public class NettyServer {
    /** A lock for start and stop operations. */
    private final Object startStopLock = new Object();

    /** {@link NioServerSocketChannel} bootstrapper. */
    private final ServerBootstrap bootstrap;

    /** Socket accepter event loop group. */
    private final NioEventLoopGroup bossGroup;

    /** Socket handler event loop group. */
    private final NioEventLoopGroup workerGroup;

    /** Server socket configuration. */
    private final NetworkView configuration;

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /** Incoming message listener. */
    private final BiConsumer<SocketAddress, NetworkMessage> messageListener;

    /** Handshake manager. */
    private final Supplier<HandshakeManager> handshakeManager;

    /** Server start future. */
    private CompletableFuture<Void> serverStartFuture;

    /** Server socket channel. */
    @Nullable
    private volatile ServerChannel channel;

    /** Server close future. */
    private CompletableFuture<Void> serverCloseFuture;

    /** New connections listener. */
    private final Consumer<NettySender> newConnectionListener;

    /** Flag indicating if {@link #stop()} has been called. */
    private boolean stopped = false;

    /**
     * Constructor.
     *
     * @param consistentId Consistent id.
     * @param configuration Server configuration.
     * @param handshakeManager Handshake manager supplier.
     * @param newConnectionListener New connections listener.
     * @param messageListener Message listener.
     * @param serializationRegistry Serialization registry.
     */
    public NettyServer(
        String consistentId,
        NetworkView configuration,
        Supplier<HandshakeManager> handshakeManager,
        Consumer<NettySender> newConnectionListener,
        BiConsumer<SocketAddress, NetworkMessage> messageListener,
        MessageSerializationRegistry serializationRegistry
    ) {
        this(
            consistentId,
            new ServerBootstrap(),
            configuration,
            handshakeManager,
            newConnectionListener,
            messageListener,
            serializationRegistry
        );
    }

    /**
     * Constructor.
     *
     * @param consistentId Consistent id.
     * @param bootstrap Server bootstrap.
     * @param configuration Server configuration.
     * @param handshakeManager Handshake manager supplier.
     * @param newConnectionListener New connections listener.
     * @param messageListener Message listener.
     * @param serializationRegistry Serialization registry.
     */
    public NettyServer(
        String consistentId,
        ServerBootstrap bootstrap,
        NetworkView configuration,
        Supplier<HandshakeManager> handshakeManager,
        Consumer<NettySender> newConnectionListener,
        BiConsumer<SocketAddress, NetworkMessage> messageListener,
        MessageSerializationRegistry serializationRegistry
    ) {
        this.bootstrap = bootstrap;
        this.configuration = configuration;
        this.handshakeManager = handshakeManager;
        this.newConnectionListener = newConnectionListener;
        this.messageListener = messageListener;
        this.serializationRegistry = serializationRegistry;
        this.bossGroup = NamedNioEventLoopGroup.create(consistentId + "-srv-accept");
        this.workerGroup = NamedNioEventLoopGroup.create(consistentId + "-srv-worker");
        serverCloseFuture = CompletableFuture.allOf(
            NettyUtils.toCompletableFuture(bossGroup.terminationFuture()),
            NettyUtils.toCompletableFuture(workerGroup.terminationFuture())
        );
    }

    /**
     * Starts the server.
     *
     * @return Future that resolves when the server is successfully started.
     */
    public CompletableFuture<Void> start() {
        synchronized (startStopLock) {
            if (stopped)
                throw new IgniteInternalException("Attempted to start an already stopped server");

            if (serverStartFuture != null)
                throw new IgniteInternalException("Attempted to start an already started server");

            InboundView inboundConfiguration = configuration.inbound();

            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    /** {@inheritDoc} */
                    @Override public void initChannel(SocketChannel ch) {
                        // Get handshake manager for the new channel.
                        HandshakeManager manager = handshakeManager.get();

                        ch.pipeline().addLast(
                            /*
                             * Decoder that uses the MessageReader
                             * to read chunked data.
                             */
                            new InboundDecoder(serializationRegistry),
                            // Handshake handler.
                            new HandshakeHandler(manager),
                            // Handles decoded NetworkMessages.
                            new MessageHandler(messageListener),
                            /*
                             * Encoder that uses the MessageWriter
                             * to write chunked data.
                             */
                            new ChunkedWriteHandler(),
                            // Converts NetworkMessage to a ChunkedNetworkMessageInput
                            new OutboundEncoder(serializationRegistry),
                            new IoExceptionSuppressingHandler()
                        );

                        manager.handshakeFuture().thenAccept(newConnectionListener);
                    }
                })
                /*
                 * The maximum queue length for incoming connection indications (a request to connect) is set
                 * to the backlog parameter. If a connection indication arrives when the queue is full,
                 * the connection is refused.
                 */
                .option(ChannelOption.SO_BACKLOG, inboundConfiguration.soBacklog())
                .option(ChannelOption.SO_REUSEADDR, inboundConfiguration.soReuseAddr())
                /*
                 * When the keepalive option is set for a TCP socket and no data has been exchanged across the socket
                 * in either direction for 2 hours (NOTE: the actual value is implementation dependent),
                 * TCP automatically sends a keepalive probe to the peer.
                 */
                .childOption(ChannelOption.SO_KEEPALIVE, inboundConfiguration.soKeepAlive())
                /*
                 * Specify a linger-on-close timeout. This option disables/enables immediate return from a close()
                 * of a TCP Socket. Enabling this option with a non-zero Integer timeout means that a close() will
                 * block pending the transmission and acknowledgement of all data written to the peer, at which point
                 * the socket is closed gracefully. Upon reaching the linger timeout, the socket is closed forcefully,
                 * with a TCP RST. Enabling the option with a timeout of zero does a forceful close immediately.
                 * If the specified timeout value exceeds 65,535 it will be reduced to 65,535.
                 */
                .childOption(ChannelOption.SO_LINGER, inboundConfiguration.soLinger())
                /*
                 * Disable Nagle's algorithm for this connection. Written data to the network is not buffered pending
                 * acknowledgement of previously written data. Valid for TCP only. Setting this option reduces
                 * network latency and and delivery time for small messages.
                 * For more information, see Socket#setTcpNoDelay(boolean)
                 * and https://en.wikipedia.org/wiki/Nagle%27s_algorithm.
                 */
                .childOption(ChannelOption.TCP_NODELAY, inboundConfiguration.tcpNoDelay());

            int port = configuration.port();
            int portRange = configuration.portRange();

            var bindFuture = new CompletableFuture<Channel>();

            tryBind(port, port + portRange, bindFuture);

            serverStartFuture = bindFuture
                .handle((channel, err) -> {
                    synchronized (startStopLock) {
                        CompletableFuture<Void> workerCloseFuture = serverCloseFuture;

                        if (channel != null) {
                            CompletableFuture<Void> channelCloseFuture = NettyUtils.toCompletableFuture(channel.closeFuture())
                                // Shutdown event loops on channel close.
                                .whenComplete((v, err0) -> shutdownEventLoopGroups());

                            serverCloseFuture = CompletableFuture.allOf(channelCloseFuture, workerCloseFuture);
                        }

                        this.channel = (ServerChannel) channel;

                        // Shutdown event loops if the server has failed to start or has been stopped.
                        if (err != null || stopped) {
                            shutdownEventLoopGroups();

                            return workerCloseFuture.handle((unused, throwable) -> {
                                Throwable stopErr = err != null ? err : new CancellationException("Server was stopped");

                                if (throwable != null)
                                    stopErr.addSuppressed(throwable);

                                return CompletableFuture.<Void>failedFuture(stopErr);
                            }).thenCompose(Function.identity());
                        }
                        else
                            return CompletableFuture.<Void>completedFuture(null);
                    }
                })
                .thenCompose(Function.identity());

            return serverStartFuture;
        }
    }

    /**
     * Try bind this server to a port.
     *
     * @param port Target port.
     * @param endPort Last port that server can be bound to.
     * @param fut Future.
     */
    private void tryBind(int port, int endPort, CompletableFuture<Channel> fut) {
        if (port > endPort)
            fut.completeExceptionally(new IllegalStateException("No available port in range"));

        bootstrap.bind(port).addListener((ChannelFuture future) -> {
            if (future.isSuccess()) {
                fut.complete(future.channel());
            } else if (future.isCancelled()) {
                fut.cancel(true);
            } else {
                tryBind(port + 1, endPort, fut);
            }
        });
    }

    /**
     * @return Gets the local address of the server.
     */
    public SocketAddress address() {
        return channel.localAddress();
    }

    /**
     * Stops the server.
     *
     * @return Future that is resolved when the server's channel has closed or an already completed future
     * for a subsequent call.
     */
    public CompletableFuture<Void> stop() {
        synchronized (startStopLock) {
            if (stopped)
                return CompletableFuture.completedFuture(null);

            stopped = true;

            if (serverStartFuture == null)
                return CompletableFuture.completedFuture(null);

            return serverStartFuture.handle((unused, throwable) -> {
               if (channel != null)
                   channel.close();

               return serverCloseFuture;
            }).thenCompose(Function.identity());
        }
    }

    /**
     * Shutdown event loops.
     */
    private void shutdownEventLoopGroups() {
        // TODO: IGNITE-14538 quietPeriod and timeout should be configurable.
        bossGroup.shutdownGracefully(0L, 15, TimeUnit.SECONDS);
        workerGroup.shutdownGracefully(0L, 15, TimeUnit.SECONDS);
    }

    /**
     * @return {@code true} if the server is running, {@code false} otherwise.
     */
    @TestOnly
    public boolean isRunning() {
        return channel != null && channel.isOpen() && !bossGroup.isShuttingDown() && !workerGroup.isShuttingDown();
    }

    /**
     * @return Accepter event loop group.
     */
    @TestOnly
    public NioEventLoopGroup getBossGroup() {
        return bossGroup;
    }

    /**
     * @return Worker event loop group.
     */
    @TestOnly
    public NioEventLoopGroup getWorkerGroup() {
        return workerGroup;
    }
}
