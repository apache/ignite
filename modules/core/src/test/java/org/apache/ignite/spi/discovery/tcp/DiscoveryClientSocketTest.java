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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Check a ssl socket configuration which used in discovery.
 */
public class DiscoveryClientSocketTest {
    /** Port to listen. */
    public static final int PORT_TO_LNSR = 12346;

    /** Host for bind. */
    public static final String HOST = "localhost";

    /** SSL server socket factory. */
    private SSLServerSocketFactory sslSrvSockFactory;

    /** SSL socket factory. */
    private SocketFactory sslSockFactory;

    /** Fake TCP discovery SPI. */
    private TcpDiscoverySpi fakeTcpDiscoverySpi;

    /**
     * Configure SSL and Discovery.
     */
    @Before
    public void before() {
        SslContextFactory socketFactory = (SslContextFactory)GridTestUtils.sslTrustedFactory("node01", "trustone");
        SSLContext sslCtx = socketFactory.create();

        sslSrvSockFactory = sslCtx.getServerSocketFactory();
        sslSockFactory = sslCtx.getSocketFactory();
        fakeTcpDiscoverySpi = new TcpDiscoverySpi();

        fakeTcpDiscoverySpi.setSoLinger(1);
    }

    /**
     * It creates a SSL socket server and client for checks correctness closing when write exceed read.
     *
     * @throws Exception If failed.
     */
    @Test
    public void sslSocketTest() throws Exception {
        try (ServerSocket listen = sslSrvSockFactory.createServerSocket(PORT_TO_LNSR)) {
            System.out.println("Server started.");

            IgniteInternalFuture clientFut = GridTestUtils.runAsync(this::startSslClient);

            Socket connection = listen.accept();

            try {
                fakeTcpDiscoverySpi.configureSocketOptions(connection);

                InputStream in = connection.getInputStream();
                OutputStream out = connection.getOutputStream();

                readHadshake(connection);

                connection.getOutputStream().write(U.IGNITE_HEADER);

                clientFut.get(10_000);
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                U.closeQuiet(connection);

                fail("Can't wait connection closed from client side.");
            }
            catch (Exception e) {
                U.closeQuiet(connection);

                System.out.println("Ex: " + e.getMessage() + " (Socket closed)");
            }
        }
    }

    /**
     * Reads handshake bytes and checks correctness.
     *
     * @param connection Socket connection.
     * @throws IOException If have some issue happens in time read from socket.
     */
    public void readHadshake(Socket connection) throws IOException {
        byte[] buf = new byte[4];
        int read = 0;

        while (read < buf.length) {
            int r = connection.getInputStream().read(buf, read, buf.length - read);

            if (r >= 0)
                read += r;
            else
                fail("Failed to read from socket.");
        }

        assertEquals("Handshake did not pass, readed bytes: " + read, Arrays.asList(U.IGNITE_HEADER), Arrays.asList(U.IGNITE_HEADER));
    }

    /**
     * Test starts ssl client socket and writes data until socket's write blocking. When the socket is blocking on write
     * tries to close it.
     */
    public void startSslClient() {
        try (Socket clientSocket = sslSockFactory.createSocket(HOST, PORT_TO_LNSR)) {
            System.out.println("Client started.");

            fakeTcpDiscoverySpi.configureSocketOptions(clientSocket);

            long handshakeStartTime = System.currentTimeMillis();

            //need to send message in order to ssl handshake passed.
            clientSocket.getOutputStream().write(U.IGNITE_HEADER);

            readHadshake(clientSocket);

            long handshakeInterval = System.currentTimeMillis() - handshakeStartTime;

            System.out.println("Handshake time: " + handshakeInterval + "ms");

            int iter = 0;

            try {
                while (true) {
                    iter++;

                    IgniteInternalFuture writeFut = GridTestUtils.runAsync(() -> {
                        try {
                            clientSocket.getOutputStream().write(new byte[4 * 1024]);
                        }
                        catch (IOException e) {
                            assertEquals("Socket closed", e.getMessage());
                        }
                    });

                    writeFut.get(10 * handshakeInterval);
                }
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                System.out.println("Socket stuck on write, when passed " + (iter * 4) + "KB through itself.");
            }

            System.out.println("Try to close a socket."); //see in try-catch-resource
        }
        catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
