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

package org.apache.ignite.client.handler;

import java.net.BindException;
import java.net.SocketAddress;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.ignite.client.proto.ClientMessageDecoder;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.Nullable;

/**
 * Client handler module maintains TCP endpoint for thin client connections.
 */
public class ClientHandlerModule implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ClientHandlerModule.class);

    /** Configuration registry. */
    private final ConfigurationRegistry registry;

    /** Ignite tables API. */
    private final IgniteTables igniteTables;

    /** Netty channel. */
    private Channel channel;

    /**
     * Constructor.
     *
     * @param igniteTables Ignite.
     * @param registry Configuration registry.
     */
    public ClientHandlerModule(IgniteTables igniteTables, ConfigurationRegistry registry) {
        assert igniteTables != null;
        assert registry != null;

        this.igniteTables = igniteTables;
        this.registry = registry;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        if (channel != null)
            throw new IgniteException("ClientHandlerModule is already started.");

        try {
            channel = startEndpoint().channel();
        }
        catch (InterruptedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        if (channel != null) {
            channel.close().await();

            channel = null;
        }
    }

    /**
     * Returns the local address where this handler is bound to.
     *
     * @return the local address of this module, or {@code null} if this module is not started.
     */
    @Nullable public SocketAddress localAddress() {
        return channel == null ? null : channel.localAddress();
    }

    /**
     * Starts the endpoint.
     *
     * @return Channel future.
     * @throws InterruptedException If thread has been interrupted during the start.
     * @throws IgniteException When startup has failed.
     */
    private ChannelFuture startEndpoint() throws InterruptedException {
        var configuration = registry.getConfiguration(ClientConnectorConfiguration.KEY).value();

        int desiredPort = configuration.port();
        int portRange = configuration.portRange();

        int port = 0;
        Channel ch = null;

        // TODO: Reuse Netty infrastructure from network module IGNITE-15307.
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();

        b.group(eventLoopGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<>() {
                @Override
                protected void initChannel(Channel ch) {
                    ch.pipeline().addLast(
                            new ClientMessageDecoder(),
                            new ClientInboundMessageHandler(igniteTables));
                }
            })
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.TCP_NODELAY, true);

        for (int portCandidate = desiredPort; portCandidate <= desiredPort + portRange; portCandidate++) {
            ChannelFuture bindRes = b.bind(portCandidate).await();

            if (bindRes.isSuccess()) {
                ch = bindRes.channel();
                ch.closeFuture().addListener((ChannelFutureListener) fut -> eventLoopGroup.shutdownGracefully());

                port = portCandidate;
                break;
            }
            else if (!(bindRes.cause() instanceof BindException)) {
                eventLoopGroup.shutdownGracefully();
                throw new IgniteException(bindRes.cause());
            }
        }

        if (ch == null) {
            String msg = "Cannot start thin client connector endpoint. " +
                "All ports in range [" + desiredPort + ", " + (desiredPort + portRange) + "] are in use.";

            LOG.error(msg);

            eventLoopGroup.shutdownGracefully();

            throw new IgniteException(msg);
        }

        LOG.info("Thin client protocol started successfully on port " + port);

        return ch.closeFuture();
    }
}
