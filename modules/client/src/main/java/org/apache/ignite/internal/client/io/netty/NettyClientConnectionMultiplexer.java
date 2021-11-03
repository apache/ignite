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

package org.apache.ignite.internal.client.io.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.internal.client.io.ClientConnection;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.io.ClientConnectionStateHandler;
import org.apache.ignite.internal.client.io.ClientMessageHandler;
import org.apache.ignite.internal.client.proto.ClientMessageDecoder;

/**
 * Netty-based multiplexer.
 */
public class NettyClientConnectionMultiplexer implements ClientConnectionMultiplexer {
    /**
     *
     */
    private final NioEventLoopGroup workerGroup;

    /**
     *
     */
    private final Bootstrap bootstrap;

    /**
     * Constructor.
     */
    public NettyClientConnectionMultiplexer() {
        workerGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
    }

    /** {@inheritDoc} */
    @Override
    public void start(IgniteClientConfiguration clientCfg) {
        try {
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) clientCfg.connectTimeout());
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(
                            new ClientMessageDecoder(),
                            new NettyClientMessageHandler());
                }
            });

        } catch (Throwable t) {
            workerGroup.shutdownGracefully();

            throw t;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        workerGroup.shutdownGracefully();
    }

    /** {@inheritDoc} */
    @Override
    public ClientConnection open(InetSocketAddress addr,
            ClientMessageHandler msgHnd,
            ClientConnectionStateHandler stateHnd)
            throws IgniteClientConnectionException {

        // TODO: Async startup IGNITE-15357.
        ChannelFuture f = bootstrap.connect(addr).syncUninterruptibly();

        return new NettyClientConnection(f.channel(), msgHnd, stateHnd);
    }
}
