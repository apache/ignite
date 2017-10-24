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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestThread;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Client-based discovery SPI test with non-Ignite servers.
 */
public class TcpDiscoveryWithWrongServerTest extends GridCommonAbstractTest {
    /** non-Ignite Server port */
    private final static int SERVER_PORT = 47500;

    /** non-Ignite Server socket */
    private ServerSocket srvSock = null;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singleton("127.0.0.1:" + Integer.toString(SERVER_PORT) + ".." +
            Integer.toString(SERVER_PORT + 2)));

        cfg.setDiscoverySpi(new TcpDiscoverySpiWithOrderedIps().setIpFinder(ipFinder));

        if (igniteInstanceName.startsWith("client"))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopTcpThread();
        stopAllGrids();
    }

    /**
     * Starts tcp test thread
     * @param workerFactory one of workerFactory
     */
    private void startTcpThread(final WorkerFactory workerFactory) {
        try {
            srvSock = new ServerSocket(SERVER_PORT);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected TcpServer exception " + e.getMessage());
            return;
        }

        new GridTestThread(new Runnable() {
            @Override public void run() {
                try {
                    while(!Thread.currentThread().isInterrupted()) {
                        Socket clientSock = srvSock.accept();
                        new GridTestThread(workerFactory.newWorker(clientSock)).start();
                    }
                }
                catch (Exception e) {
                    if (!srvSock.isClosed())
                        e.printStackTrace();
                }
            }
        }).start();
    }

    /**
     * Stops tcp test thread
     * @throws IOException IOException
     */
    private void stopTcpThread() throws IOException {
        if (srvSock != null)
            if (!srvSock.isClosed())
                srvSock.close();
    }

    /**
     * Test that Client successfully ignores wrong responses during Discovery Handshake Procedure.
     *
     * @throws Exception in case of error.
     */
    public void testWrongHandshakeResponse() throws Exception {
        startTcpThread(new SomeResponseWorkerFactory());

        simpleTest();
    }

    /**
     * Test that Client successfully ignores wrong responses during Discovery Handshake Procedure.
     *
     * @throws Exception in case of error.
     */
    public void testNoHandshakeResponse() throws Exception {
        startTcpThread(new NoResponseWorkerFactory());

        simpleTest();
    }

    /**
     * Test that Client successfully ignores when server closes sockets after Discovery Handshake Request.
     *
     * @throws Exception in case of error.
     */
    public void testDisconnectOnRequest() throws Exception {
        startTcpThread(new DisconnectOnRequestWorkerFactory());

        simpleTest();
    }

    /**
     * Test that Client successfully ignores when server closes sockets immediately.
     *
     * @throws Exception in case of error.
     */
    public void testEarlyDisconnect() throws Exception {
        startTcpThread(new EarlyDisconnectWorkerFactory());

        simpleTest();
    }

    /**
     * Some simple sanity check with the Server and Client
     * It is expected that both client and server could successfully perform Discovery Procedure when there is
     * unknown (test) server in the ipFinder list
     */
    private void simpleTest() {
        try {
            Ignite srv = startGrid("server");
            Ignite client = startGrid("client");
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
     * Factory for SomeResponseWorker
     */
    private class SomeResponseWorkerFactory implements WorkerFactory {
        /** {@inheritDoc} */
        @Override public Runnable newWorker(Socket clientSock) {
            return new SomeResponseWorker(clientSock);
        }
    }

    /**
     * Factory for NoResponseWorker
     */
    private class NoResponseWorkerFactory implements WorkerFactory {
        /** {@inheritDoc} */
        @Override public Runnable newWorker(Socket clientSock) {
            return new NoResponseWorker(clientSock);
        }
    }

    /**
     * Factory for DisconnectOnRequestWorker
     */
    private class DisconnectOnRequestWorkerFactory implements WorkerFactory {
        /** {@inheritDoc} */
        @Override public Runnable newWorker(Socket clientSock) {
            return new DisconnectOnRequestWorker(clientSock);
        }
    }

    /**
     * Factory for EarlyDisconnectWorker
     */
    private class EarlyDisconnectWorkerFactory implements WorkerFactory {
        /** {@inheritDoc} */
        @Override public Runnable newWorker(Socket clientSock) {
            return new EarlyDisconnectWorker(clientSock);
        }
    }

    /**
     * AbstractWorker
     */
    private abstract class AbstractWorker implements Runnable {
        /** Client socket. */
        Socket clientSock = null;

        /**
         * @param clientSock Client socket.
         */
        AbstractWorker(Socket clientSock) {
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
            } catch (IOException e) {
                //report exception somewhere.
                e.printStackTrace();
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
     * SomeResponseWorker
     */
    private class SomeResponseWorker extends AbstractWorker {
        /**
         * @param clientSock Client socket.
         */
        SomeResponseWorker(Socket clientSock) {
            super(clientSock);
        }

        /** {@inheritDoc} */
        @Override public void action(InputStream input, OutputStream output) throws IOException {
            output.write("Some response".getBytes());
            log.error("TEST: Some response was sent to " + clientSock.getRemoteSocketAddress());
        }
    }

    /**
     * NoResponseWorker
     */
    private class NoResponseWorker extends AbstractWorker {
        /**
         * @param clientSock Client socket.
         */
        NoResponseWorker(Socket clientSock) {
            super(clientSock);
        }

        /** {@inheritDoc} */
        @Override public void action(InputStream input, OutputStream output) throws IOException {
            log.error("TEST: No response was sent to " + clientSock.getRemoteSocketAddress());
        }
    }

    /**
     * DisconnectOnRequestWorker
     */
    private class DisconnectOnRequestWorker extends AbstractWorker {
        /**
         * @param clientSock Client socket.
         */
        DisconnectOnRequestWorker(Socket clientSock) {
            super(clientSock);
        }

        /** {@inheritDoc} */
        @Override public void action(InputStream input, OutputStream output) throws IOException {
            clientSock.close();
            log.error("TEST: Socket closed for " + clientSock.getRemoteSocketAddress());
        }
    }

    /**
     * EarlyDisconnectWorker
     */
    private class EarlyDisconnectWorker extends AbstractWorker {
        /**
         * @param clientSock Client socket.
         */
        EarlyDisconnectWorker(Socket clientSock) {
            super(clientSock);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                clientSock.close();
                log.error("TEST: Socket closed for " + clientSock.getRemoteSocketAddress());
            } catch (IOException e) {
                //report exception somewhere.
                e.printStackTrace();
            }
        }

        /** {@inheritDoc} */
        @Override public void action(InputStream input, OutputStream output) throws IOException {
            // No-op
        }
    }

    /**
     * TcpDiscoverySpi with non-shuffled resolved IP Addresses. We should ensure that out test non-Ignite server
     * is the first element of the addresses list
     */
    class TcpDiscoverySpiWithOrderedIps extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected Collection<InetSocketAddress> resolvedAddresses() throws IgniteSpiException {
            Collection<InetSocketAddress> shuffled = super.resolvedAddresses();
            ArrayList<InetSocketAddress> res = new ArrayList<>(shuffled);
            res.sort(new Comparator<InetSocketAddress>() {
                @Override public int compare(InetSocketAddress o1, InetSocketAddress o2) {
                    return o1.toString().compareTo(o2.toString());
                }
            });
            return res;
        }
    }
}
