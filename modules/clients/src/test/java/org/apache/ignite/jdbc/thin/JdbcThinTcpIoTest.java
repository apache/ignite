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

package org.apache.ignite.jdbc.thin;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.jdbc.thin.ConnectionPropertiesImpl;
import org.apache.ignite.internal.jdbc.thin.JdbcThinTcpIo;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for JdbcThinTcpIo.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinTcpIoTest extends GridCommonAbstractTest {
    /** Server port range. */
    private static final int[] SERVER_PORT_RANGE = {59000, 59020};

    /** Inaccessible addresses. */
    private static final String INACCESSIBLE_ADDRESSES[] = {"123.45.67.89", "123.45.67.90"};

    /**
     * Create test server socket.
     *
     * @return Server socket.
     */
    private ServerSocket createServerSocket(CountDownLatch checkConnection) {
        for (int port = SERVER_PORT_RANGE[0]; port <= SERVER_PORT_RANGE[1]; port++) {
            try {
                final ServerSocket serverSock = new ServerSocket(port);

                System.out.println("Created server socket: " + port);

                if (checkConnection != null) {
                    new Thread(new Runnable() {
                        @Override public void run() {
                            try (Socket sock = serverSock.accept()) {
                                checkConnection.countDown();
                            }
                            catch (IOException ignore) {
                                // No-op
                            }
                        }
                    }).start();
                }

                return serverSock;
            }
            catch (IOException ignore) {
                // No-op
            }
        }

        fail("Server socket wasn't created.");

        return null;
    }

    /**
     * Create JdbcThinTcpIo instance.
     *
     * @param addrs IP-addresses.
     * @param port Server socket port.
     * @return JdbcThinTcpIo instance.
     * @throws SQLException On connection error or reject.
     */
    private JdbcThinTcpIo createTcpIo(String[] addrs, int port) throws SQLException {
        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();

        connProps.setAddresses(new HostAndPortRange[]{new HostAndPortRange("test.domain.name", port, port)});

        return new JdbcThinTcpIo(connProps) {
            @Override protected InetAddress[] getAllAddressesByHost(String host) throws UnknownHostException {
                InetAddress[] addresses = new InetAddress[addrs.length];

                for (int i = 0; i < addrs.length; i++)
                    addresses[i] = InetAddress.getByName(addrs[i]);

                return addresses;
            }

            @Override public void handshake(ClientListenerProtocolVersion ver) {
                // Skip handshake.
            }
        };
    }

    /**
     * Test connection to host which has inaccessible A-records.
     *
     * @throws SQLException On connection error or reject.
     * @throws IOException On IO error in handshake.
     */
    public void testHostWithManyAddresses() throws SQLException, IOException, InterruptedException {
        CountDownLatch connectionAccepted = new CountDownLatch(1);

        try (ServerSocket sock = createServerSocket(connectionAccepted)) {
            String[] addrs = {INACCESSIBLE_ADDRESSES[0], "127.0.0.1", INACCESSIBLE_ADDRESSES[1]};

            JdbcThinTcpIo jdbcThinTcpIo = createTcpIo(addrs, sock.getLocalPort());

            try {
                jdbcThinTcpIo.start(500);

                // Check connection
                assertTrue(connectionAccepted.await(1000, TimeUnit.MILLISECONDS));
            }
            finally {
                jdbcThinTcpIo.close();
            }
        }
    }

    /**
     * Test exception text (should contain inaccessible ip addresses list).
     *
     * @throws SQLException On connection error or reject.
     * @throws IOException On IO error in handshake.
     */
    public void testExceptionMessage() throws SQLException, IOException {
        try (ServerSocket sock = createServerSocket(null)) {
            String[] addrs = {INACCESSIBLE_ADDRESSES[0], INACCESSIBLE_ADDRESSES[1]};

            JdbcThinTcpIo jdbcThinTcpIo = createTcpIo(addrs, sock.getLocalPort());

            Throwable throwable = GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    jdbcThinTcpIo.start(500);
                    return null;
                }
            }, SQLException.class, null);

            String msg = throwable.getMessage();

            for (Throwable sup : throwable.getSuppressed())
                msg += " " + sup.getMessage();

            for (String addr : addrs)
                assertTrue(String.format("Exception message should contain %s", addr), msg.contains(addr));
        }
    }
}
