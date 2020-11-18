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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.thin.io.ClientConnection;
import org.apache.ignite.internal.client.thin.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioFilter;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLogger;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

/**
 * Client connection multiplexer based on {@link org.apache.ignite.internal.util.nio.GridNioServer}.
 */
public class GridNioServerClientConnectionMultiplexer implements ClientConnectionMultiplexer {
    /** */
    private static final int CLIENT_MODE_PORT = -1;

    /** */
    private final GridNioServer<byte[]> srv;

    public GridNioServerClientConnectionMultiplexer() {
        IgniteLogger gridLog = new JavaLogger(false);

        GridNioFilter[] filters;

        GridNioFilter codecFilter = new GridNioCodecFilter(new GridNioParser() {
            @Override
            public @Nullable Object decode(GridNioSession ses, ByteBuffer buf) throws IOException, IgniteCheckedException {
                // TODO: See ClientMessage.readFrom
                return buf.remaining();
            }

            @Override
            public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
                return ByteBuffer.wrap((byte[])msg);
            }
        }, gridLog, false);

//        if (sslCtx != null) {
//            GridNioSslFilter sslFilter = new GridNioSslFilter(sslCtx, true, ByteOrder.nativeOrder(), gridLog);
//
//            sslFilter.directMode(false);
//
//            filters = new GridNioFilter[]{codecFilter, sslFilter};
//        }
//        else
        filters = new GridNioFilter[]{codecFilter};

        try {
            srv = GridNioServer.<byte[]>builder()
                    .address(InetAddress.getLoopbackAddress()) // TODO: Remove?
                    .port(CLIENT_MODE_PORT)
                    .listener(new GridNioServerListener<byte[]>() {
                        @Override
                        public void onConnected(GridNioSession ses) {
                            System.out.println("Connect");
                        }

                        @Override
                        public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                            System.out.println("Disconnect");
                        }

                        @Override
                        public void onMessageSent(GridNioSession ses, byte[] msg) {

                        }

                        @Override
                        public void onMessage(GridNioSession ses, byte[] msg) {
                            // TODO: Handle response for a connection denoted by ses
                            // Call decoder here.
                        }

                        @Override
                        public void onSessionWriteTimeout(GridNioSession ses) {

                        }

                        @Override
                        public void onSessionIdleTimeout(GridNioSession ses) {

                        }

                        @Override
                        public void onFailure(FailureType failureType, Throwable failure) {
                            System.out.println("Fail");
                        }
                    })
                    .filters(filters)
                    .logger(gridLog)
                    .selectorCount(1)
                    .sendQueueLimit(1024)
                    .byteOrder(ByteOrder.nativeOrder())
                    .directBuffer(true)
                    .directMode(false)
                    .socketReceiveBufferSize(0)
                    .socketSendBufferSize(0)
                    .idleTimeout(Long.MAX_VALUE)
                    .igniteInstanceName("thinClient")
                    .serverName("tcp-client")
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
    @Override public ClientConnection open(InetSocketAddress address) {
        java.nio.channels.SocketChannel ch = java.nio.channels.SocketChannel.open();
        Socket sock = ch.socket();

        sock.connect(new InetSocketAddress("127.0.0.1", 10800), Integer.MAX_VALUE);

        Map<Integer, Object> meta = new HashMap<>();

        // TODO: What does async param mean?
        GridNioFuture sesFut = srv.createSession(ch, meta, false, new CI1<IgniteInternalFuture<GridNioSession>>() {
            @Override
            public void apply(IgniteInternalFuture<GridNioSession> sesFut) {
                System.out.println("Session created: " + sesFut.result().toString());
//                try {
//                    sesFut.get().send(null);
//                } catch (IgniteCheckedException e) {
//                    e.printStackTrace();
//                }
            }
        });

        GridNioSession ses = (GridNioSession)sesFut.get();

        // Socket send is handled by worker threads.
        GridNioFuture<?> sendFut = ses.send(new byte[0]);
        sendFut.listen(f -> {
            System.out.println(f.isDone());
        });
        // sendFut.get();

        return null;
    }
}
