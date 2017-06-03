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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.jdbc.thin.JdbcThinUtils;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * Connection test.
 */
public class JdbcThinConnectionSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(@NotNull String name) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"EmptyTryBlock", "unused"})
    public void testDefaults() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            // No-op.
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/")) {
            // No-op.
        }
    }

    /**
     * Test invalid endpoint.
     *
     * @throws Exception If failed.
     */
    public void testInvalidEndpoint() throws Exception {
        assertInvalid("jdbc:ignite:thin://", "Host name is empty");
        assertInvalid("jdbc:ignite:thin://:10000", "Host name is empty");
        assertInvalid("jdbc:ignite:thin://     :10000", "Host name is empty");

        assertInvalid("jdbc:ignite:thin://127.0.0.1:-1", "Invalid port");
        assertInvalid("jdbc:ignite:thin://127.0.0.1:0", "Invalid port");
        assertInvalid("jdbc:ignite:thin://127.0.0.1:100000", "Invalid port");
    }

    /**
     * Test invalid socket buffer sizes.
     *
     * @throws Exception If failed.
     */
    public void testSocketBuffers() throws Exception {
        assertInvalid("jdbc:ignite:thin://127.0.0.1?socketSendBuffer=-1",
            "Property cannot be negative [name=" + JdbcThinUtils.PARAM_SOCK_SND_BUF);

        assertInvalid("jdbc:ignite:thin://127.0.0.1?socketReceiveBuffer=-1",
            "Property cannot be negative [name=" + JdbcThinUtils.PARAM_SOCK_RCV_BUF);

        // Get default values.
        int dfltSndBuf;
        int dfltRcvBuf;

        try (Socket sock = new Socket()) {
            sock.connect(new InetSocketAddress("127.0.0.1", JdbcThinUtils.DFLT_PORT));

            dfltSndBuf = sock.getSendBufferSize();
            dfltRcvBuf = sock.getReceiveBufferSize();
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            assertEquals(dfltSndBuf, socket(conn).getSendBufferSize());
            assertEquals(dfltRcvBuf, socket(conn).getReceiveBufferSize());
        }

        int customSndBuf = dfltSndBuf / 2;
        int customRcvBuf = dfltRcvBuf / 4;

        // Note that SO_* options are hints, so we check that value is equals to either what we set or to default.
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?" +
            "socketSendBuffer=" + customSndBuf)) {
            int sndBuf = socket(conn).getSendBufferSize();
            assertTrue(sndBuf == customSndBuf || sndBuf == dfltSndBuf);

            assertEquals(dfltRcvBuf, socket(conn).getReceiveBufferSize());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?" +
            "socketReceiveBuffer=" + customRcvBuf)) {
            assertEquals(dfltSndBuf, socket(conn).getSendBufferSize());

            int rcvBuf = socket(conn).getReceiveBufferSize();
            assertTrue(rcvBuf == customRcvBuf || rcvBuf == dfltRcvBuf);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?" +
            "socketSendBuffer=" + customSndBuf + "&socketReceiveBuffer=" + customRcvBuf)) {
            int sndBuf = socket(conn).getSendBufferSize();
            assertTrue(sndBuf == customSndBuf || sndBuf == dfltSndBuf);

            int rcvBuf = socket(conn).getReceiveBufferSize();
            assertTrue(rcvBuf == customRcvBuf || rcvBuf == dfltRcvBuf);
        }
    }

    /**
     * Test TCP no delay property handling.
     *
     * @throws Exception If failed.
     */
    public void testPropertyTcpNoDelay() throws Exception {
        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=0",
            "Failed to parse boolean property [name=" + JdbcThinUtils.PARAM_TCP_NO_DELAY);

        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=1",
            "Failed to parse boolean property [name=" + JdbcThinUtils.PARAM_TCP_NO_DELAY);

        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=false1",
            "Failed to parse boolean property [name=" + JdbcThinUtils.PARAM_TCP_NO_DELAY);

        assertInvalid("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=true1",
            "Failed to parse boolean property [name=" + JdbcThinUtils.PARAM_TCP_NO_DELAY);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            assertTrue(socket(conn).getTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=true")) {
            assertTrue(socket(conn).getTcpNoDelay());
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?tcpNoDelay=false")) {
            assertFalse(socket(conn).getTcpNoDelay());
        }
    }

    /**
     * Get client socket for connection.
     *
     * @param conn Connection.
     * @return Socket.
     * @throws Exception If failed.
     */
    private static Socket socket(Connection conn) throws Exception {
        JdbcThinConnection conn0 = conn.unwrap(JdbcThinConnection.class);

        return conn0.io().endpoint().clientSocket();
    }

    /**
     * Assert that provided URL is invalid.
     *
     * @param url URL.
     * @param errMsg Error message.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void assertInvalid(final String url, String errMsg) {
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                DriverManager.getConnection(url);

                return null;
            }
        }, SQLException.class, errMsg);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    public void testClose() throws Exception {
        final Connection conn;

        try (Connection conn0 = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            conn = conn0;

            assert conn != null;
            assert !conn.isClosed();
        }

        assert conn.isClosed();

        assert !conn.isValid(2): "Connection must be closed";

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                conn.isValid(-2);

                return null;
            }
        }, SQLException.class, "Invalid timeout");
    }
}