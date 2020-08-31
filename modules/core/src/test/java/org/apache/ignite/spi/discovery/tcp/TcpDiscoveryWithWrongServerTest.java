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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestThread;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Client-based discovery SPI test with non-Ignite servers.
 */
public class TcpDiscoveryWithWrongServerTest extends GridCommonAbstractTest {
    /** Non-Ignite Server port #1. */
    private static final int SERVER_PORT = 47500;

    /** Non-Ignite Server port #2. */
    private static final int LAST_SERVER_PORT = SERVER_PORT + 5;

    /** Non-Ignite Server sockets. */
    private List<ServerSocket> srvSocks = new ArrayList<>();

    /** Count of accepted connections to non-Ignite Server. */
    private AtomicInteger connCnt = new AtomicInteger(0);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singleton("127.0.0.1:" + Integer.toString(SERVER_PORT) + ".." +
            Integer.toString(LAST_SERVER_PORT)));

        cfg.setDiscoverySpi(new TcpDiscoverySpiWithOrderedIps().setIpFinder(ipFinder));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopTcpThreads();

        stopAllGrids();

        super.afterTest();
    }

    /**
     * Starts tcp test thread
     * @param workerFactory one of WorkerFactory
     */
    private void startTcpThread(final WorkerFactory workerFactory, final int port) throws Exception {
        final ServerSocket srvSock = new ServerSocket(port, 10, InetAddress.getByName("127.0.0.1"));

        srvSocks.add(srvSock);

        new GridTestThread(new Runnable() {
            @Override public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Socket clientSock = srvSock.accept();

                        connCnt.getAndIncrement();

                        // Create a new thread for socket connection.
                        new GridTestThread(workerFactory.newWorker(clientSock)).start();
                    }
                }
                catch (Exception e) {
                    if (!srvSock.isClosed())
                        log.error("Unexpected error", e);
                }
            }
        }).start();
    }

    /**
     * Stops tcp test thread
     * @throws IOException IOException
     */
    private void stopTcpThreads() throws IOException {
        for (ServerSocket srvSock: srvSocks)
            if (!srvSock.isClosed())
                srvSock.close();
    }

    /**
     * Test that Client successfully ignores wrong responses during Discovery Handshake Procedure.
     *
     * @throws Exception in case of error.
     */
    @Test
    public void testWrongHandshakeResponse() throws Exception {
        startTcpThread(new SomeResponseWorker(), SERVER_PORT);
        startTcpThread(new SomeResponseWorker(), LAST_SERVER_PORT);

        simpleTest();
    }

    /**
     * Test that Client successfully ignores wrong responses during Discovery Handshake Procedure.
     *
     * @throws Exception in case of error.
     */
    @Test
    public void testNoHandshakeResponse() throws Exception {
        startTcpThread(new NoResponseWorker(), SERVER_PORT);
        startTcpThread(new NoResponseWorker(), LAST_SERVER_PORT);

        simpleTest();
    }

    /**
     * Test that Client successfully ignores when server closes sockets after Discovery Handshake Request.
     *
     * @throws Exception in case of error.
     */
    @Test
    public void testDisconnectOnRequest() throws Exception {
        startTcpThread(new DisconnectOnRequestWorker(), SERVER_PORT);
        startTcpThread(new DisconnectOnRequestWorker(), LAST_SERVER_PORT);

        simpleTest();
    }

    /**
     * Test that Client successfully ignores when server closes sockets immediately.
     *
     * @throws Exception in case of error.
     */
    @Test
    public void testEarlyDisconnect() throws Exception {
        startTcpThread(new EarlyDisconnectWorker(), SERVER_PORT);
        startTcpThread(new EarlyDisconnectWorker(), LAST_SERVER_PORT);

        simpleTest();
    }

    /**
     * Some simple sanity check with the Server and Client
     * It is expected that both client and server could successfully perform Discovery Procedure when there is
     * unknown (test) server in the ipFinder list.
     */
    private void simpleTest() {
        try {
            Ignite srv = startGrid("server");
            Ignite client = startClientGrid("client");

            awaitPartitionMapExchange();

            assertEquals(2, srv.cluster().nodes().size());
            assertEquals(2, client.cluster().nodes().size());
            assertTrue(connCnt.get() >= 2);

            srv.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

            assertEquals(1, client.getOrCreateCache(DEFAULT_CACHE_NAME).get(1));
        }
        catch (Exception e) {
            fail("Failed with unexpected exception: " + e.getMessage());
        }
    }

    /**
     * Just a factory for runnable workers
     */
    private interface WorkerFactory {
        /**
         * Creates a new worker for socket
         * @param clientSock socket for worker
         * @return runnable Worker
         */
        Runnable newWorker(Socket clientSock);
    }

    /**
     * SocketWorker
     */
    private abstract class SocketWorker implements Runnable {
        /** Client socket. */
        Socket clientSock;

        /**
         * @param clientSock Client socket.
         */
        SocketWorker(Socket clientSock) {
            this.clientSock = clientSock;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                InputStream input = clientSock.getInputStream();
                OutputStream output = clientSock.getOutputStream();
                byte[] buf = new byte[1024];

                while (!clientSock.isClosed() && input.read(buf) > 0)
                    action(input, output);

                if (!clientSock.isClosed())
                    clientSock.close();
            }
            catch (IOException e) {
                log.error("Unexpected error", e);
            }
        }

        /**
         * @param input socket input stream
         * @param output socket output stream
         * @throws IOException IOException
         */
        public abstract void action(InputStream input, OutputStream output) throws IOException;
    }

    /**
     * SomeResponseWorker.
     */
    private class SomeResponseWorker implements WorkerFactory {
        /** {@inheritDoc} */
        @Override public Runnable newWorker(Socket clientSock) {
            return new SocketWorker(clientSock) {
                @Override public void action(InputStream input, OutputStream output) throws IOException {
                    output.write("Some response".getBytes());

                    log.error("TEST: Some response was sent to " + clientSock.getRemoteSocketAddress());
                }
            };
        }
    }

    /**
     * NoResponseWorker.
     */
    private class NoResponseWorker implements WorkerFactory {
        /** {@inheritDoc} */
        @Override public Runnable newWorker(Socket clientSock) {
            return new SocketWorker(clientSock) {
                @Override public void action(InputStream input, OutputStream output) throws IOException {
                    log.error("TEST: No response was sent to " + clientSock.getRemoteSocketAddress());
               }
            };
        }
    }

    /**
     * DisconnectOnRequestWorker.
     */
    private class DisconnectOnRequestWorker implements WorkerFactory {
        /** {@inheritDoc} */
        @Override public Runnable newWorker(Socket clientSock) {
            return new SocketWorker(clientSock) {
                @Override public void action(InputStream input, OutputStream output) throws IOException {
                    clientSock.close();

                    log.error("TEST: Socket closed for " + clientSock.getRemoteSocketAddress());
                }
            };
        }
    }

    /**
     * EarlyDisconnectWorker.
     */
    private class EarlyDisconnectWorker implements WorkerFactory {
        /** {@inheritDoc} */
        @Override public Runnable newWorker(Socket clientSock) {
            return new SocketWorker(clientSock) {
                @Override public void action(InputStream input, OutputStream output) throws IOException {
                    // No-op
                }

                @Override public void run() {
                    try {
                        clientSock.close();

                        log.error("TEST: Socket closed for " + clientSock.getRemoteSocketAddress());
                    }
                    catch (IOException e) {
                        log.error("Unexpected error", e);
                    }
                }
            };
        }
    }

    /**
     * TcpDiscoverySpi with non-shuffled resolved IP addresses. We should ensure that in this test non-Ignite server
     * is the first element of the addresses list
     */
    class TcpDiscoverySpiWithOrderedIps extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected Collection<InetSocketAddress> resolvedAddresses() throws IgniteSpiException {
            Collection<InetSocketAddress> shuffled = super.resolvedAddresses();
            List<InetSocketAddress> res = new ArrayList<>(shuffled);

            Collections.sort(res, new Comparator<InetSocketAddress>() {
                @Override public int compare(InetSocketAddress o1, InetSocketAddress o2) {
                    return o1.toString().compareTo(o2.toString());
                }
            });

            return res;
        }
    }
}
