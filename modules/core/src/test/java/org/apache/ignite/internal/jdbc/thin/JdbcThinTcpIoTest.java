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

package org.apache.ignite.internal.jdbc.thin;

import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.sql.SQLException;

/**
 * Tests for JdbcThinTcpIo.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinTcpIoTest extends GridCommonAbstractTest {
    /** Server port range */
    private static final int[] SERVER_PORT_RANGE = {59000, 59020};

    /** Inaccessible addresses */
    private static final String INACCESSIBLE_ADDRESSES[] = {"123.45.67.89", "123.45.67.90"};

    /** Create test server socket. */
    private ServerSocket createServerSocket() {
        for (int port = SERVER_PORT_RANGE[0]; port <= SERVER_PORT_RANGE[1]; port++) {
            try {
                ServerSocket sock = new ServerSocket(port);
                System.out.println("Created server socket [port=" + port + "]");
                return sock;
            } catch(IOException ignore) {}
        }
        return null;
    }

    /** Create JdbcThinTcpIo instance. */
    private JdbcThinTcpIo createTcpIo(String[] addrs, int port) throws SQLException {
        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();

        connProps.setHost("test.domain.name");
        connProps.setPort(port);

        return new JdbcThinTcpIo(connProps) {
            @Override
            protected InetAddress[] getAllAddressesByHost(String host) throws UnknownHostException {
                InetAddress[] addresses = new InetAddress[addrs.length];

                for (int i = 0; i < addrs.length; i++)
                    addresses[i] = InetAddress.getByName(addrs[i]);

                return addresses;
            }

            @Override
            public void handshake(ClientListenerProtocolVersion ver) {
                // Skip handshake.
            }
        };
    }

    /** Test connection to host which has inaccessible A-records. */
    public void testHostWithManyAddresses() throws SQLException, IOException {
        try(ServerSocket sock = createServerSocket()) {
            String[] addrs = {INACCESSIBLE_ADDRESSES[0], "127.0.0.1", INACCESSIBLE_ADDRESSES[1]};

            JdbcThinTcpIo jdbcThinTcpIo = createTcpIo(addrs, sock.getLocalPort());

            try {
                jdbcThinTcpIo.start(500);
                // Connection is established. Test is passed.
            } finally {
                jdbcThinTcpIo.close();
            }
        }
    }

    /** Test exception text (should contain inaccessible ip addresses list). */
    public void testExceptionMessage() throws SQLException, IOException {
        try(ServerSocket sock = createServerSocket()) {
            String[] addrs = {INACCESSIBLE_ADDRESSES[0], INACCESSIBLE_ADDRESSES[1]};

            JdbcThinTcpIo jdbcThinTcpIo = createTcpIo(addrs, sock.getLocalPort());

            try {
                jdbcThinTcpIo.start(500);
                fail("Socket shouldn't be connected.");
            } catch(SQLException exception) {
                String msg = exception.getMessage();

                for (String addr : addrs) {
                    assertTrue(String.format("Exception message should contain %s", addr), msg.contains(addrs[0]));
                }
            } finally {
                jdbcThinTcpIo.close();
            }
        }
    }
}
