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

package org.apache.ignite.internal.client.thin.io.gridnioserver;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.client.thin.ClientSslUtils;
import org.apache.ignite.internal.client.thin.io.ClientConnection;
import org.apache.ignite.internal.client.thin.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.thin.io.ClientConnectionStateHandler;
import org.apache.ignite.internal.client.thin.io.ClientMessageHandler;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioFilter;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioFutureImpl;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Client connection multiplexer based on {@link org.apache.ignite.internal.util.nio.GridNioServer}.
 */
public class GridNioClientConnectionMultiplexer implements ClientConnectionMultiplexer {
    /** Worker thread prefix. */
    private static final String THREAD_PREFIX = "thin-client-channel";

    /** */
    private static final int CLIENT_MODE_PORT = -1;

    /** */
    private final GridNioServer<ByteBuffer> srv;

    /** */
    private final SSLContext sslCtx;

    public GridNioClientConnectionMultiplexer(ClientConfiguration cfg) {
        IgniteLogger gridLog = new NullLogger();

        GridNioFilter[] filters;

        GridNioFilter codecFilter = new GridNioCodecFilter(new GridNioClientParser(), gridLog, false);

        sslCtx = ClientSslUtils.getSslContext(cfg);

        if (sslCtx != null) {
            GridNioSslFilter sslFilter = new GridNioSslFilter(sslCtx, true, ByteOrder.nativeOrder(), gridLog);
            sslFilter.directMode(false);
            filters = new GridNioFilter[]{codecFilter, sslFilter};
        } else
            filters = new GridNioFilter[]{codecFilter};

        try {
            srv = GridNioServer.<ByteBuffer>builder()
                    .port(CLIENT_MODE_PORT)
                    .listener(new GridNioServerListener<ByteBuffer>() {
                        @Override public void onConnected(GridNioSession ses) {
                            // No-op.
                        }

                        @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                            GridNioClientConnection conn = ses.meta(GridNioClientConnection.SES_META_CONN);

                            // Conn can be null when connection fails during initialization in open method.
                            if (conn != null)
                                conn.onDisconnected(e);
                        }

                        @Override
                        public void onMessageSent(GridNioSession ses, ByteBuffer msg) {

                        }

                        @Override
                        public void onMessage(GridNioSession ses, ByteBuffer msg) {
                            GridNioClientConnection conn = ses.meta(GridNioClientConnection.SES_META_CONN);

                            conn.onMessage(msg);
                        }

                        @Override
                        public void onSessionWriteTimeout(GridNioSession ses) {

                        }

                        @Override
                        public void onSessionIdleTimeout(GridNioSession ses) {

                        }

                        @Override
                        public void onFailure(FailureType failureType, Throwable failure) {
                            // TODO: ???
                            System.out.println("Fail");
                        }
                    })
                    .filters(filters)
                    .logger(gridLog)
                    // TODO: Review settings below
                    .selectorCount(1) // TODO: Get from settings
                    .byteOrder(ByteOrder.nativeOrder())
                    .directBuffer(true)
                    .directMode(false)
                    .igniteInstanceName("thinClient")
                    .serverName(THREAD_PREFIX)
                    .writeTimeout(cfg.getTimeout() > 0 ? cfg.getTimeout() : -1)
                    .build();
        } catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() {
        srv.start();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        srv.stop();
    }

    /** {@inheritDoc} */
    @Override public ClientConnection open(InetSocketAddress addr,
                                           ClientMessageHandler msgHnd,
                                           ClientConnectionStateHandler stateHnd)
            throws ClientConnectionException {
        try {
            java.nio.channels.SocketChannel ch = java.nio.channels.SocketChannel.open();
            ch.socket().connect(new InetSocketAddress(addr.getHostName(), addr.getPort()), Integer.MAX_VALUE);

            Map<Integer, Object> meta = new HashMap<>();
            GridNioFuture<?> sslHandshakeFut = null;

            if (sslCtx != null) {
                sslHandshakeFut = new GridNioFutureImpl<>(null);

                meta.put(GridNioSslFilter.HANDSHAKE_FUT_META_KEY, sslHandshakeFut);
            }

            GridNioSession ses = srv.createSession(ch, meta, false, null).get();

            if (sslHandshakeFut != null)
                sslHandshakeFut.get();

            return new GridNioClientConnection(ses, msgHnd, stateHnd);
        } catch (Exception e) {
            throw new ClientConnectionException(e.getMessage(), e);
        }
    }
}
