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
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * We emulate that client receive message about joining to topology earlier than some server nodes in topology.
 * And make this client connect to such servers.
 * To emulate this we connect client to second node in topology and pause sending message about joining finishing to
 * third node.
 */
public class IgniteClientConnectTest extends GridCommonAbstractTest {
    /** Latch to stop message sending. */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** Start client flag. */
    private final AtomicBoolean clientJustStarted = new AtomicBoolean(false);

    /** Failure detection timeout. */
    private int failureDetectionTimeout = -1;

    /** Node add finished delay. */
    private int nodeAddFinishedDelay = 5_000;

    /** Connection timeout. */
    private long connTimeout = -1;

    /** Maxx connection timeout. */
    private long maxxConnTimeout = -1;

    /** Recon count. */
    private int reconCnt = -1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestTcpDiscoverySpi disco = new TestTcpDiscoverySpi();

        if ("client".equals(igniteInstanceName)) {
            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

            ipFinder.registerAddresses(Collections.singleton(new InetSocketAddress(InetAddress.getLoopbackAddress(), 47501)));

            disco.setIpFinder(ipFinder);

            if (failureDetectionTimeout != -1)
                cfg.setFailureDetectionTimeout(failureDetectionTimeout);

            if (connTimeout != -1) {
                TcpCommunicationSpi tcpCommSpi = (TcpCommunicationSpi)cfg.getCommunicationSpi();

                tcpCommSpi.setConnectTimeout(connTimeout);
                tcpCommSpi.setMaxConnectTimeout(maxxConnTimeout);
                tcpCommSpi.setReconnectCount(reconCnt);
            }
        }
        else {
            disco.setIpFinder(sharedStaticIpFinder);

            cfg.setFailureDetectionTimeout(60_000);
        }

        disco.setJoinTimeout(2 * 60_000);
        disco.setSocketTimeout(1000);
        disco.setNetworkTimeout(6_000);

        cfg.setNetworkSendRetryCount(1);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration cacheConfiguration = new CacheConfiguration()
                .setName(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAffinity(new RendezvousAffinityFunction(false, 8))
                .setBackups(0);

        cfg.setCacheConfiguration(cacheConfiguration);

        return cfg;
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientConnectToBigTopology() throws Exception {
        failureDetectionTimeout = -1;
        connTimeout = -1;

        testClientConnectToBigTopology0();
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailureDetectionTimeoutReached() throws Exception {
        failureDetectionTimeout = 1000;
        connTimeout = -1;

        try {
            testClientConnectToBigTopology0();
        }
        catch (CacheException e) {
            assertTrue(e.getCause().getMessage().contains("Failed to send message"));
        }
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCustomTimeoutReached() throws Exception {
        failureDetectionTimeout = 1000;

        connTimeout = 1000;
        maxxConnTimeout = 3000;
        reconCnt = 3;

        try {
            testClientConnectToBigTopology0();
        }
        catch (CacheException e) {
            assertTrue(e.getCause().getMessage().contains("Failed to send message"));
        }
    }

    /**
     *
     * @throws Exception In case of error.
     */
    public void testClientConnectToBigTopology0() throws Exception {
        Ignite ignite = startGrids(3);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < 80; i++) {
            cache.put(i, i);

            keys.add(i);
        }

        TcpDiscoveryImpl discovery = ((TestTcpDiscoverySpi) ignite.configuration().getDiscoverySpi()).discovery();

        assertTrue(discovery instanceof ServerImpl);

        IgniteConfiguration clientCfg = getConfiguration("client");

        clientJustStarted.set(true);

        IgniteEx client = startClientGrid(clientCfg);

        latch.countDown();

        client.cache(DEFAULT_CACHE_NAME).getAll(keys);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg, long timeout) throws IOException,
                IgniteCheckedException {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                if (msg.senderNodeId() != null && clientJustStarted.get())
                    try {
                        latch.await();

                        Thread.sleep(nodeAddFinishedDelay);
                    } catch (InterruptedException e) {
                        fail("Unexpected interrupt on nodeAddFinishedDelay");
                    }

                super.writeToSocket(sock, out, msg, timeout);
            }
            else
                super.writeToSocket(sock, out, msg, timeout);
        }

        /**
         *
         */
        TcpDiscoveryImpl discovery() {
            return impl;
        }
    }
}
