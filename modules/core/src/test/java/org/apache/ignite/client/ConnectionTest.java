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

package org.apache.ignite.client;

import java.io.OutputStream;
import java.net.Socket;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Checks if it can connect to a valid address from the node address list.
 */
public class ConnectionTest {
    /** IPv4 default host. */
    public static final String IPv4_HOST = "127.0.0.1";

    /** IPv6 default host. */
    public static final String IPv6_HOST = "::1";

    /** */
    @Test(expected = org.apache.ignite.client.ClientException.class)
    public void testEmptyNodeAddress() throws Exception {
        testConnection(IPv4_HOST, "");
    }

    /** */
    @Test(expected = org.apache.ignite.client.ClientException.class)
    public void testNullNodeAddress() throws Exception {
        testConnection(IPv4_HOST, null);
    }

    /** */
    @Test(expected = org.apache.ignite.client.ClientException.class)
    public void testNullNodeAddresses() throws Exception {
        testConnection(IPv4_HOST, null, null);
    }

    /** */
    @Test
    public void testValidNodeAddresses() throws Exception {
        testConnection(IPv4_HOST, Config.SERVER);
    }

    /** */
    @Test(expected = org.apache.ignite.client.ClientConnectionException.class)
    public void testInvalidNodeAddresses() throws Exception {
        testConnection(IPv4_HOST, "127.0.0.1:47500", "127.0.0.1:10801");
    }

    /** */
    @Test
    public void testValidInvalidNodeAddressesMix() throws Exception {
        testConnection(IPv4_HOST, "127.0.0.1:47500", "127.0.0.1:10801", Config.SERVER);
    }

    /** */
    @Ignore("IPv6 is not enabled by default on some systems.")
    @Test
    public void testIPv6NodeAddresses() throws Exception {
        testConnection(IPv6_HOST, "[::1]:10800");
    }

    /** */
    @Test(expected = org.apache.ignite.client.ClientConnectionException.class)
    public void testInvalidBigHandshakeMessage() throws Exception {
        char[] data = new char[1024 * 1024 * 128];
        String userName = new String(data);

        testConnectionWithUsername(userName, Config.SERVER);
    }

    /** */
    @Test
    public void testValidBigHandshakeMessage() throws Exception {
        char[] data = new char[1024 * 65];
        String userName = new String(data);

        testConnectionWithUsername(userName, Config.SERVER);
    }

    /** */
    @Test
    public void testHandshakeTooLargeServerDropsConnection() throws Exception {
        try (LocalIgniteCluster ignored = LocalIgniteCluster.start(1, IPv4_HOST)) {
            Socket clientSock = new Socket(IPv4_HOST, 10800);
            OutputStream stream = clientSock.getOutputStream();

            stream.write(new byte[]{1, 1, 1, 1});
            stream.flush();

            // Read returns -1 when end of stream has been reached, blocks otherwise.
            assertEquals(-1, clientSock.getInputStream().read());
        }
    }

    /** */
    @Test
    public void testNegativeMessageSizeDropsConnection() throws Exception {
        try (LocalIgniteCluster ignored = LocalIgniteCluster.start(1, IPv4_HOST)) {
            Socket clientSock = new Socket(IPv4_HOST, 10800);
            OutputStream stream = clientSock.getOutputStream();

            byte b = (byte)255;
            stream.write(new byte[]{b, b, b, b});
            stream.flush();

            // Read returns -1 when end of stream has been reached, blocks otherwise.
            assertEquals(-1, clientSock.getInputStream().read());
        }
    }

    /** */
    @Test
    public void testInvalidHandshakeHeaderDropsConnection() throws Exception {
        try (LocalIgniteCluster ignored = LocalIgniteCluster.start(1, IPv4_HOST)) {
            Socket clientSock = new Socket(IPv4_HOST, 10800);
            OutputStream stream = clientSock.getOutputStream();

            stream.write(new byte[]{10, 0, 0, 0, 42, 42, 42});
            stream.flush();

            assertEquals(-1, clientSock.getInputStream().read());
        }
    }

    /**
     * @param addrs Addresses to connect.
     * @param host LocalIgniteCluster host.
     */
    private void testConnection(String host, String... addrs) throws Exception {
        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1, host);
             IgniteClient client = Ignition.startClient(new ClientConfiguration()
                     .setAddresses(addrs))) {
        }
    }

    /**
     * @param userName User name.
     * @param addrs Addresses to connect.
     */
    private void testConnectionWithUsername(String userName, String... addrs) throws Exception {
        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(new ClientConfiguration()
                 .setAddresses(addrs)
                 .setUserName(userName))) {
        }
    }
}
