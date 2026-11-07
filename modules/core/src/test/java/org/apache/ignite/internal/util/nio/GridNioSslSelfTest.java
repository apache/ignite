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

package org.apache.ignite.internal.util.nio;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.nio.impl.MockNioSession;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for new NIO server with SSL enabled.
 */
public class GridNioSslSelfTest extends GridNioSelfTest {
    /** Test SSL context. */
    private static SSLContext sslCtx;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        sslCtx = GridTestUtils.sslContext();
    }

    /** */
    @Test
    public void testContextLookupFailureCountsOneRejection() throws Exception {
        Runnable rejectedSessions = mock(Runnable.class);
        GridNioSslFilter filter = new GridNioSslFilter(() -> {
            throw new IgniteException("Test SSL context lookup failure");
        }, false, ByteOrder.nativeOrder(), log, null, rejectedSessions);
        GridNioSession ses = new MockNioSession();

        filter.previousFilter(mock(GridNioFilter.class));

        assertThrows(IgniteCheckedException.class, () -> filter.onSessionOpened(ses));

        filter.onSessionClosed(ses);

        verify(rejectedSessions).run();
    }

    /** */
    @Test
    public void testServerAcceptsConnectionAfterSslFactoryFailure() throws Exception {
        AtomicBoolean failNextLookup = new AtomicBoolean();
        CountDownLatch serverConnected = new CountDownLatch(1);

        Factory<SSLContext> sslCtxFactory = () -> {
            if (failNextLookup.getAndSet(false))
                throw new IgniteException("Test SSL context lookup failure");

            return sslCtx;
        };

        GridNioServer<ByteBuffer> srv = GridNioServer.<ByteBuffer>builder()
            .address(U.getLocalHost())
            .port(GridTestUtils.getFreeCommPort())
            .listener(new GridNioServerListenerAdapter<>() {
                /** {@inheritDoc} */
                @Override public void onConnected(GridNioSession ses) {
                    serverConnected.countDown();
                }

                /** {@inheritDoc} */
                @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
                    // No-op.
                }

                /** {@inheritDoc} */
                @Override public void onMessage(GridNioSession ses, ByteBuffer msg) {
                    // No-op.
                }
            })
            .logger(log)
            .selectorCount(1)
            .filters(U.sslFilter(sslCtxFactory, false, ByteOrder.nativeOrder(), log, null))
            .build();

        srv.start();

        try {
            failNextLookup.set(true);

            InetSocketAddress addr = new InetSocketAddress(U.getLocalHost(), srv.port());

            try (Socket sock = new Socket()) {
                sock.connect(addr, 5_000);
                sock.setSoTimeout(5_000);

                assertEquals(-1, sock.getInputStream().read());
                assertFalse(failNextLookup.get());
            }

            try (SSLSocket sock = (SSLSocket)createSocket()) {
                sock.connect(addr, 5_000);
                sock.setSoTimeout(5_000);
                sock.startHandshake();

                assertTrue(serverConnected.await(5, TimeUnit.SECONDS));
            }
        }
        finally {
            srv.stop();
        }
    }

    /** {@inheritDoc} */
    @Override protected Socket createSocket() throws IgniteCheckedException {
        try {
            return sslCtx.getSocketFactory().createSocket();
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected GridNioServer.Builder<?> serverBuilder(int port,
        GridNioParser parser,
        GridNioServerListener lsnr
    ) throws Exception {
        return GridNioServer.builder()
            .address(U.getLocalHost())
            .port(port)
            .listener(lsnr)
            .logger(log)
            .selectorCount(2)
            .igniteInstanceName("nio-test-grid")
            .tcpNoDelay(false)
            .directBuffer(true)
            .byteOrder(ByteOrder.nativeOrder())
            .socketSendBufferSize(0)
            .socketReceiveBufferSize(0)
            .sendQueueLimit(0)
            .filters(
                new GridNioCodecFilter(parser, log, false),
                new GridNioSslFilter(() -> sslCtx, true, ByteOrder.nativeOrder(), log, null, null));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testWriteTimeout() throws Exception {
        // Skip base test because it enables "skipWrite" mode in the GridNioServer
        // which makes SSL handshake impossible.
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testAsyncSendReceive() throws Exception {
        // No-op, do not want to mess with SSL channel.
    }
}
