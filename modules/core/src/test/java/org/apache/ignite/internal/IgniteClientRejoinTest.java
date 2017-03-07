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

package org.apache.ignite.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteCouldReconnectCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests client to be able restore connection to cluster if coordination is not available.
 */
public class IgniteClientRejoinTest extends GridCommonAbstractTest {
    /** Keys. */
    public static final int KEYS = 100;

    /** Static IP finder. */
    public static final TcpDiscoveryIpFinder finder = new TcpDiscoveryVmIpFinder(true);

    /** Block. */
    private volatile boolean block;

    /** Coordinator. */
    private volatile ClusterNode crd;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        boolean client = gridName.contains("client");

        log.error("Grid name: " + gridName + ", client: " + client);

        TcpCommunicationSpi comSpi = new TcpCommunicationSpi(client);

        comSpi.setIdleConnectionTimeout(100);
        comSpi.setReconnectCount(1);
        cfg.setCommunicationSpi(comSpi);

        DiscoverySpi dspi = new DiscoverySpi(client);
        dspi.setIpFinder(finder);

        if (client)
            dspi.setJoinTimeout(TimeUnit.MINUTES.toMillis(5));

        cfg.setDiscoverySpi(dspi);
        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectOnStart() throws Exception {
        Ignite srv1 = startGrid("server1");

        crd = ((IgniteKernal)srv1).localNode();

        log.info("Coordinator node: " + crd);

        Ignite srv2 = startGrid("server2");

        // Block sending messages to coordinator.
        block = true;

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Random rnd = new Random();

                U.sleep((rnd.nextInt(15) + 30) * 10_000);

                block = false;

                log.info("ALLOW connection to coordinator.");

                return true;
            }
        });

        Ignite client = startGrid("client");

        assertTrue(fut.get());

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache("some");

        for (int i = 0; i < 100; i++) {
            cache.put(i, i);

            assertEquals(i, (int)cache.get(i));
        }

        Collection<ClusterNode> clients = client.cluster().forClients().nodes();

        assertEquals("Clients: " + clients, 1, clients.size());
        assertEquals(1, srv1.cluster().forClients().nodes().size());
        assertEquals(1, srv2.cluster().forClients().nodes().size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnect() throws Exception {
        final String CACHE_NAME = "some";

        final Ignite srv1 = startGrid("server1");

        crd = ((IgniteKernal)srv1).localNode();

        Ignite srv2 = startGrid("server2");

        Ignite client = startGrid("client");

        final IgniteCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

        final Integer keyOnCrd = primaryKey(srv1.cache(CACHE_NAME));

        for (int i = 0; i < 100; i++) {
            cache.put(i, i);

            assertEquals(i, (int)cache.get(i));
        }

        awaitPartitionMapExchange();

        log.error("!!!! Blocked message to coordinator.");

        // Block sending messages to coordinator.
        block = true;

//        IgniteInternalFuture<Object> f = GridTestUtils.runAsync(new Callable<Object>() {
//            @Override public Object call() throws Exception {
//                try {
//                    cache.put(keyOnCrd, -42);
//                }
//                catch (Exception ignore) {
//                    ignore.printStackTrace();
//                }
//
//                return null;
//            }
//        });
//
//        try {
//            f.get(100);
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }

        IgniteInternalFuture<Boolean> fut = GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Random rnd = new Random();

                int ms = 30_000;

                log.error("Block time: " + ms);

                U.sleep(ms);

                block = false;

                log.info("ALLOW connection to coordinator.");

                return true;
            }
        });

        U.sleep(5_000);

        startGrid("server3");

        fut.get();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return srv1.cluster().forClients().nodes().size() == 1;
            }
        }, 100_000);

        IgniteCache<Object, Object> cache0 = client.cache(CACHE_NAME);

        int cnt = 0;

        cache0.put(1, 42);

        log.error("Retries count: " + cnt);

        assertTrue(fut.get());

        Collection<ClusterNode> clients = client.cluster().forClients().nodes();

        assertEquals("Clients: " + clients, 1, clients.size());
        assertEquals(1, srv1.cluster().forClients().nodes().size());
        assertEquals(1, srv2.cluster().forClients().nodes().size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyClientsReconnectOnStart() throws Exception {
        final int SERVERS_CNT = 2;

        Ignite srv1 = startGrid("server1");

        crd = ((IgniteKernal)srv1).localNode();

        Ignite srv2 = startGrid("server2");

        block = true;

        List<IgniteInternalFuture<Ignite>> futs = new ArrayList<>();

        final CountDownLatch latch = new CountDownLatch(1);

        final int CLIENTS_CNT = 5;

        for (int i = 0; i < CLIENTS_CNT; i++) {
            final int idx = i;

            IgniteInternalFuture<Ignite> fut = GridTestUtils.runAsync(new Callable<Ignite>() {
                @Override public Ignite call() throws Exception {
                    latch.await();

                    return startGrid("client" + idx);
                }
            });

            futs.add(fut);
        }

        GridTestUtils.runAsync(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                latch.countDown();

                Random rnd = new Random();

                U.sleep((rnd.nextInt(15) + 15) * 1000);

                block = false;

                System.out.println(">>> ALLOW connection to coordinator.");

                return true;
            }
        });

        for (IgniteInternalFuture<Ignite> clientFut : futs) {
            Ignite client = clientFut.get();

            IgniteCache<Integer, Integer> cache = client.getOrCreateCache(client.name());

            for (int i = 0; i < KEYS; i++) {
                cache.put(i, i);

                assertEquals(i, (int)cache.get(i));
            }

            assertEquals(SERVERS_CNT, client.cluster().forServers().nodes().size());
        }

        assertEquals(CLIENTS_CNT, srv1.cluster().forClients().nodes().size());
        assertEquals(CLIENTS_CNT, srv2.cluster().forClients().nodes().size());
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60_000;
    }

    /** {@inheritDoc} */
    @Override public boolean isDebug() {
        return true;
    }

    /**
     *
     */
    private class TcpCommunicationSpi extends org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi {
        /** Server. */
        private final boolean client;

        /**
         * @param client {@code True} if server.
         */
        public TcpCommunicationSpi(boolean client) {
            this.client = client;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            if (client) {
                if (block && node.id().equals(crd.id())) // Failed send to coordinator.
                    throw new IgniteSpiException(new SocketException("Test communication exception"));
            }

            super.sendMessage(node, msg);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (client) {
                if (block && node.id().equals(crd.id()))
                    throw new IgniteSpiException(new SocketException("Test communication exception"));
            }

            super.sendMessage(node, msg, ackC);
        }
    }

    /**
     *
     */
    private class DiscoverySpi extends TcpDiscoverySpi {
        /** Server. */
        private final boolean client;

        /**
         * @param client {@code True} if server.
         */
        public DiscoverySpi(boolean client) {
            this.client = client;
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg, byte[] data,
            long timeout) throws IOException {
            if (client) {
                if (block && sock.getPort() == 47500) // Failed send to coordinator
                    throw new SocketException("Test discovery exception");
            }

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (client) {
                if (block && sock.getPort() == 47500)
                    throw new SocketException("Test discovery exception");
            }

            super.writeToSocket(sock, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (client) {
                if (block && sock.getPort() == 47500)
                    throw new SocketException("Test discovery exception");
            }

            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
            long timeout) throws IOException {
            if (client) {
                if (block && sock.getPort() == 47500)
                    throw new SocketException("Test discovery exception");
            }

            super.writeToSocket(msg, sock, res, timeout);
        }

        /** {@inheritDoc} */
        @Override protected Socket openSocket(Socket sock, InetSocketAddress remAddr,
            IgniteSpiOperationTimeoutHelper timeoutHelper) throws IOException, IgniteSpiOperationTimeoutException {
            if (client) {
                if (block && sock.getPort() == 47500)
                    throw new SocketException("Test discovery exception");
            }

            return super.openSocket(sock, remAddr, timeoutHelper);
        }
    }
}
