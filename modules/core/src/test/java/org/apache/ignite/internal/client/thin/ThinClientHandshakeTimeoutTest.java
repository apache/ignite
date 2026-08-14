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

package org.apache.ignite.internal.client.thin;

import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;

/** */
public class ThinClientHandshakeTimeoutTest extends GridCommonAbstractTest {
    /** */
    private static final String HOST = "127.0.0.1";

    /** */
    private static final int HANDSHAKE_TIMEOUT = 2_000;

    /** */
    @Test
    public void testHandshakeTimeout() throws Exception {
        try (TestServer srv = new TestServer()) {
            assertClientConnectionFailed(
                clientConfiguration(srv.port(), false),
                "Failed to wait for Ignite Client handshake completion");
        }
    }

    /** */
    @Test
    public void testSslHandshakeTimeout() throws Exception {
        try (TestServer srv = new TestServer()) {
            assertClientConnectionFailed(
                clientConfiguration(srv.port(), true),
                "Failed to wait for SSL handshake completion");
        }
    }

    /** */
    @Test
    public void testConnectionIsReleasedOnHandshakeTimeout() throws Exception {
        startGrid(0);

        try (TestServer srv = new TestServer()) {
            ClientConfiguration cfg = new ClientConfiguration()
                .setAddresses(HOST + ':' + srv.port(), HOST + ':' + ClientConnectorConfiguration.DFLT_PORT)
                .setHandshakeTimeout(HANDSHAKE_TIMEOUT);

            try (IgniteClient cli = Ignition.startClient(cfg)) {
                srv.awaitConnectionAcceptedAndClosedByClient(getTestTimeout());

                assertEquals(1, cli.cluster().nodes().size());
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    private ClientConfiguration clientConfiguration(int port, boolean isSslEnabled) {
        ClientConfiguration cfg = new ClientConfiguration()
            .setAddresses(HOST + ':' + port)
            .setHandshakeTimeout(HANDSHAKE_TIMEOUT);

        if (isSslEnabled) {
            cfg.setSslMode(SslMode.REQUIRED);
            cfg.setSslContextFactory(sslTrustedFactory("thinClient", "trusttwo"));
        }

        return cfg;
    }

    /** */
    private static void assertClientConnectionFailed(ClientConfiguration cfg, String errMsg) {
        assertThrowsAnyCause(log, () -> Ignition.startClient(cfg), ClientConnectionException.class, errMsg);
    }

    /** */
    private static class TestServer implements AutoCloseable {
        /** */
        private static final int DFLT_PORT = 1024;

        /** */
        private final ServerSocket srvSock;

        /** */
        private final BlockingQueue<Socket> accepted = new LinkedBlockingQueue<>();

        /** */
        private final Thread acceptor;

        /** */
        TestServer() throws IOException {
            srvSock = createServerSocket();

            acceptor = new Thread(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted())
                        accepted.add(srvSock.accept());
                }
                catch (IOException ignored) {
                    // No-op.
                }
            }, "test-server-acceptor");

            acceptor.setDaemon(true);
            acceptor.start();
        }

        /** */
        private ServerSocket createServerSocket() throws IOException {
            int port = DFLT_PORT;

            while (true) {
                try {
                    return new ServerSocket(port, 50, InetAddress.getByName(HOST));
                }
                catch (BindException ignore) {
                    port++;

                    assertTrue(port < ClientConnectorConfiguration.DFLT_PORT);
                }
            }
        }

        /** */
        int port() {
            return srvSock.getLocalPort();
        }

        /** */
        void awaitConnectionAcceptedAndClosedByClient(long timeout) throws Exception {
            Socket sock = accepted.poll(timeout, TimeUnit.MILLISECONDS);

            assertNotNull(sock);

            sock.setSoTimeout((int)timeout);

            InputStream in = sock.getInputStream();

            while (in.read() >= 0) {
                // No-op.
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            acceptor.interrupt();

            srvSock.close();

            List<Socket> socks = new ArrayList<>();

            accepted.drainTo(socks);

            for (Socket sock : socks)
                sock.close();

            acceptor.join(5000);
        }
    }
}
