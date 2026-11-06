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
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;
import static org.junit.Assert.assertThrows;

/** Tests discovery after individual TLS setup failures. */
public class TcpDiscoverySslConnectionFailureTest extends GridCommonAbstractTest {
    /** */
    private static final int SOCK_TIMEOUT = 10_000;

    /** Discovery addresses shared within one test. */
    private final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testContextLookupFailureKeepsAccepting() throws Exception {
        FailingSslFactory factory = new FailingSslFactory();
        InetSocketAddress addr = address(startNode(0, factory));

        assertRejectsOneConnection(addr, factory, () -> {
            throw new IgniteException("Test failure looking up the SSL context");
        });
        assertAcceptsTls(addr, factory.create());
    }

    /** */
    @Test
    public void testSocketWrapFailureKeepsAccepting() throws Exception {
        FailingSslFactory factory = new FailingSslFactory();
        InetSocketAddress addr = address(startNode(0, factory));
        SSLContext ctx = factory.create();

        SSLParameters badParams = new SSLParameters();
        badParams.setProtocols(new String[]{"TestUnsupportedProtocol"});

        // This context can be obtained, but applying its parameters to the layered socket fails.
        assertRejectsOneConnection(addr, factory, () -> new SSLContextWrapper(ctx, badParams));
        assertAcceptsTls(addr, ctx);
    }

    /** */
    @Test
    public void testOutgoingSocketSetupFailure() throws Exception {
        SSLContext ctx = sslTrustedFactory("node01", "trustone").create();
        AtomicReference<SSLContext> curCtx = new AtomicReference<>(ctx);
        IgniteEx node = startNode(0, curCtx::get);
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
    @Test
    public void testInvalidProtocolPreventsFirstNodeStartup() throws Exception {
        SslContextFactory factory = (SslContextFactory)sslTrustedFactory("node01", "trustone");
        factory.setProtocols("TestUnsupportedProtocol");

        assertThrowsAnyCause(log, () -> startNode(0, factory), IllegalArgumentException.class, "TestUnsupportedProtocol");
    }

    /** */
    private IgniteEx startNode(int idx, Factory<SSLContext> factory) throws Exception {
        return startGrid(idx, cfg -> {
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
            cfg.setSslContextFactory(factory);
        });
    }

    /** */
    private static InetSocketAddress address(IgniteEx node) {
        int port = ((TcpDiscoverySpi)node.configuration().getDiscoverySpi()).getLocalPort();

        return new InetSocketAddress("127.0.0.1", port);
    }

    /** Injects a TLS failure for one accepted socket. */
    private static void assertRejectsOneConnection(
        InetSocketAddress addr,
        FailingSslFactory factory,
        Factory<SSLContext> nextCtx
    ) throws Exception {
        assertTrue(factory.nextCtx.compareAndSet(null, nextCtx));

        try (Socket sock = new Socket()) {
            sock.connect(addr, SOCK_TIMEOUT);
            sock.setSoTimeout(SOCK_TIMEOUT);

            assertEquals("Discovery must close the socket after TLS setup fails", -1, sock.getInputStream().read());
            assertNull(factory.nextCtx.get());
        }
    }

    /** */
    private static void assertAcceptsTls(InetSocketAddress addr, SSLContext ctx) throws Exception {
        try (SSLSocket sock = (SSLSocket)ctx.getSocketFactory().createSocket()) {
            sock.connect(addr, SOCK_TIMEOUT);
            sock.setSoTimeout(SOCK_TIMEOUT);
            sock.startHandshake();
        }
    }

    /** One-shot context override on the discovery accept thread. */
    private static class FailingSslFactory extends SslContextFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** One-shot context supplier. */
        private final AtomicReference<Factory<SSLContext>> nextCtx = new AtomicReference<>();

        /** */
        private FailingSslFactory() {
            char[] pwd = keyStorePassword().toCharArray();

            setKeyStoreFilePath(keyStorePath("node01"));
            setKeyStorePassword(pwd);
            setTrustStoreFilePath(keyStorePath("trustone"));
            setTrustStorePassword(pwd);
        }

        /** {@inheritDoc} */
        @Override public SSLContext create() {
            if (Thread.currentThread().getName().startsWith("tcp-disco-srvr-")) {
                Factory<SSLContext> next = nextCtx.getAndSet(null);

                if (next != null)
                    return next.create();
            }

            return super.create();
        }
    }
}
