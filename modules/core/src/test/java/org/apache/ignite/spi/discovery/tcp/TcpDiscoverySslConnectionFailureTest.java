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

package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.ssl.SSLContextWrapper;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;
import static org.junit.Assert.assertThrows;

/** Tests discovery TLS startup and connection failures. */
public class TcpDiscoverySslConnectionFailureTest extends GridCommonAbstractTest {
    /** */
    private static final int SOCK_TIMEOUT = 10_000;

    /** */
    private final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testInvalidProtocolPreventsFirstNodeStartup() throws Exception {
        SslContextFactory factory = (SslContextFactory)sslTrustedFactory("node01", "trustone");
        factory.setProtocols("TestUnsupportedProtocol");

        assertThrowsAnyCause(log, () -> startNode(factory), IllegalArgumentException.class, "TestUnsupportedProtocol");
    }

    /** */
    @Test
    public void testDiscoveryAcceptsConnectionAfterSslFactoryFailure() throws Exception {
        SSLContext ctx = sslTrustedFactory("node01", "trustone").create();
        AtomicBoolean failNextCreate = new AtomicBoolean();
        Factory<SSLContext> sslCtxFactory = () -> {
            if (failNextCreate.getAndSet(false))
                throw new IgniteException("Test SSL context creation failure");

            return ctx;
        };
        InetSocketAddress addr = discoveryAddress(startNode(sslCtxFactory));

        failNextCreate.set(true);

        connectAndAssertClosed(addr);

        assertFalse(failNextCreate.get());

        connectAndHandshake(addr, ctx);
    }

    /** */
    @Test
    public void testSocketSetupFailureKeepsAccepting() throws Exception {
        SSLContext validCtx = sslTrustedFactory("node01", "trustone").create();
        AtomicReference<SSLContext> nextCtx = new AtomicReference<>(validCtx);
        InetSocketAddress addr = discoveryAddress(startNode(() -> nextCtx.getAndSet(validCtx)));
        SSLParameters invalidParams = new SSLParameters();
        invalidParams.setProtocols(new String[]{"TestUnsupportedProtocol"});

        nextCtx.set(new SSLContextWrapper(validCtx, invalidParams));

        connectAndAssertClosed(addr);

        assertSame(validCtx, nextCtx.get());

        connectAndHandshake(addr, validCtx);
    }

    /** */
    @Test
    public void testOutgoingSocketSetupFailureIsReportedAsIoException() throws Exception {
        SSLContext ctx = sslTrustedFactory("node01", "trustone").create();
        AtomicReference<SSLContext> curCtx = new AtomicReference<>(ctx);
        IgniteEx node = startNode(curCtx::get);
        TcpDiscoverySpi spi = (TcpDiscoverySpi)node.configuration().getDiscoverySpi();

        SSLParameters badParams = new SSLParameters();
        badParams.setProtocols(new String[]{"TestUnsupportedProtocol"});
        curCtx.set(new SSLContextWrapper(ctx, badParams));

        IOException err = assertThrows(IOException.class, () -> spi.createSocket().close());

        assertTrue(err.getCause() instanceof IllegalArgumentException);

        curCtx.set(ctx);

        try (Socket sock = spi.createSocket()) {
            assertTrue(sock.isBound());
        }
    }

    /** */
    private IgniteEx startNode(Factory<SSLContext> factory) throws Exception {
        return startGrid(0, cfg -> {
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
            cfg.setSslContextFactory(factory);
        });
    }

    /** */
    private static InetSocketAddress discoveryAddress(IgniteEx node) {
        int port = ((TcpDiscoverySpi)node.configuration().getDiscoverySpi()).getLocalPort();

        return new InetSocketAddress("127.0.0.1", port);
    }

    /** */
    private static void connectAndAssertClosed(InetSocketAddress addr) throws Exception {
        try (Socket sock = new Socket()) {
            sock.connect(addr, SOCK_TIMEOUT);
            sock.setSoTimeout(SOCK_TIMEOUT);

            assertEquals("Discovery must close the socket after TLS setup fails", -1, sock.getInputStream().read());
        }
    }

    /** */
    private static void connectAndHandshake(InetSocketAddress addr, SSLContext ctx) throws Exception {
        try (SSLSocket sock = (SSLSocket)ctx.getSocketFactory().createSocket()) {
            sock.connect(addr, SOCK_TIMEOUT);
            sock.setSoTimeout(SOCK_TIMEOUT);
            sock.startHandshake();
        }
    }
}
