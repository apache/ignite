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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiMBean;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiTestUtil;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.zookeeper.ZkTestClientCnxnSocketNIO;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

/**
 * Tests for Zookeeper SPI discovery.
 */
public class ZookeeperDiscoveryClientDisconnectTest extends ZookeeperDiscoverySpiTestBase {
    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // we reduce fealure detection tp speedup failure detection on catch(Exception) clause in createTcpClient().
        cfg.setFailureDetectionTimeout(2000);
        cfg.setClientFailureDetectionTimeout(2000);

        return cfg;
    }

    /**
     * Test reproduces failure in case of client resolution failure
     * {@link org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi#createTcpClient} from server side, further
     * client reconnect and proper grid work.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnects() throws Exception {
        blockCommSpi = true;

        Ignite srv1 = startGrid("server1-block");

        IgniteEx cli = startClientGrid("client-block");

        IgniteCache<Object, Object> cache = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        assertEquals(cache.get(1), 1);

        assertEquals(1, srv1.cluster().forClients().nodes().size());

        ZookeeperDiscoverySpiMBean bean = getMxBean(srv1.name(), "SPIs",
            ZookeeperDiscoverySpi.class, ZookeeperDiscoverySpiMBean.class);

        assertNotNull(bean);

        assertEquals(0, bean.getCommErrorProcNum());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConnectionCheck() throws Exception {
        final int NODES = 5;

        startGridsMultiThreaded(NODES);

        for (int i = 0; i < NODES; i++) {
            Ignite node = ignite(i);

            TcpCommunicationSpi spi = (TcpCommunicationSpi)node.configuration().getCommunicationSpi();

            List<ClusterNode> nodes = new ArrayList<>(node.cluster().nodes());

            BitSet res = spi.checkConnection(nodes).get();

            for (int j = 0; j < NODES; j++)
                assertTrue(res.get(j));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectDisabled_ConnectionLost() throws Exception {
        clientReconnectDisabled = true;

        startGrid(0);

        sesTimeout = 3000;
        testSockNio = true;

        Ignite client = startClientGrid(1);

        final CountDownLatch latch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                latch.countDown();

                return false;
            }
        }, EventType.EVT_NODE_SEGMENTED);

        ZkTestClientCnxnSocketNIO nio = ZkTestClientCnxnSocketNIO.forNode(client);

        nio.closeSocket(true);

        try {
            ZookeeperDiscoverySpiTestHelper.waitNoAliveZkNodes(log,
                zkCluster.getConnectString(),
                Collections.singletonList(ZookeeperDiscoverySpiTestHelper.aliveZkNodePath(client)),
                10_000);
        }
        finally {
            nio.allowConnect();
        }

        assertTrue(latch.await(10, SECONDS));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServersLeft_FailOnTimeout() throws Exception {
        startGrid(0);

        final int CLIENTS = 5;

        joinTimeout = 3000;

        startClientGridsMultiThreaded(1, CLIENTS);

        waitForTopology(CLIENTS + 1);

        final CountDownLatch latch = new CountDownLatch(CLIENTS);

        for (int i = 0; i < CLIENTS; i++) {
            Ignite node = ignite(i + 1);

            node.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    latch.countDown();

                    return false;
                }
            }, EventType.EVT_NODE_SEGMENTED);
        }

        stopGrid(getTestIgniteInstanceName(0), true, false);

        assertTrue(latch.await(10, SECONDS));

        evts.clear();
    }

    /**
     *
     */
    @Test
    public void testStartNoServers_FailOnTimeout() {
        joinTimeout = 3000;

        long start = System.currentTimeMillis();

        Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startClientGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        assertTrue(System.currentTimeMillis() >= start + joinTimeout);

        IgniteSpiException spiErr = X.cause(err, IgniteSpiException.class);

        assertNotNull(spiErr);
        assertTrue(spiErr.getMessage().contains("Failed to connect to cluster within configured timeout"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartNoServer_WaitForServers1() throws Exception {
        startNoServer_WaitForServers(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartNoServer_WaitForServers2() throws Exception {
        startNoServer_WaitForServers(10_000);
    }

    /**
     * @param joinTimeout Join timeout.
     * @throws Exception If failed.
     */
    private void startNoServer_WaitForServers(long joinTimeout) throws Exception {
        this.joinTimeout = joinTimeout;

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startClientGrid(0);

                return null;
            }
        });

        U.sleep(3000);

        helper.waitSpi(getTestIgniteInstanceName(0), spis);

        startGrid(1);

        fut.get();

        waitForTopology(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisconnectOnServersLeft_1() throws Exception {
        disconnectOnServersLeft(1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisconnectOnServersLeft_2() throws Exception {
        disconnectOnServersLeft(5, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisconnectOnServersLeft_3() throws Exception {
        disconnectOnServersLeft(1, 10);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisconnectOnServersLeft_4() throws Exception {
        disconnectOnServersLeft(5, 10);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisconnectOnServersLeft_5() throws Exception {
        joinTimeout = 10_000;

        disconnectOnServersLeft(5, 10);
    }

    /**
     * @param srvs Number of servers.
     * @param clients Number of clients.
     * @throws Exception If failed.
     */
    private void disconnectOnServersLeft(int srvs, int clients) throws Exception {
        startGridsMultiThreaded(srvs);

        startClientGridsMultiThreaded(srvs, clients);

        for (int i = 0; i < GridTestUtils.SF.applyLB(5, 2); i++) {
            info("Iteration: " + i);

            final CountDownLatch disconnectLatch = new CountDownLatch(clients);
            final CountDownLatch reconnectLatch = new CountDownLatch(clients);

            IgnitePredicate<Event> p = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                        log.info("Disconnected: " + evt);

                        disconnectLatch.countDown();
                    }
                    else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                        log.info("Reconnected: " + evt);

                        reconnectLatch.countDown();

                        return false;
                    }

                    return true;
                }
            };

            for (int c = 0; c < clients; c++) {
                Ignite client = ignite(srvs + c);

                assertTrue(client.configuration().isClientMode());

                client.events().localListen(p, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);
            }

            log.info("Stop all servers.");

            GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                @Override public void apply(Integer threadIdx) {
                    stopGrid(getTestIgniteInstanceName(threadIdx), true, false);
                }
            }, srvs, "stop-server");

            ZookeeperDiscoverySpiTestHelper.waitReconnectEvent(log, disconnectLatch);

            evts.clear();

            log.info("Restart servers.");

            startGridsMultiThreaded(0, srvs);

            ZookeeperDiscoverySpiTestHelper.waitReconnectEvent(log, reconnectLatch);

            waitForTopology(srvs + clients);

            log.info("Reconnect finished.");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartNoZk() throws Exception {
        stopZkCluster();

        sesTimeout = 30_000;

        zkCluster = ZookeeperDiscoverySpiTestUtil.createTestingCluster(3);

        try {
            final AtomicInteger idx = new AtomicInteger();

            IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(idx.getAndIncrement());

                    return null;
                }
            }, 5, "start-node");

            U.sleep(5000);

            assertFalse(fut.isDone());

            zkCluster.start();

            fut.get();

            waitForTopology(5);
        }
        finally {
            zkCluster.start();
        }
    }
}
