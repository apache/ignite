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
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;


/**
 * We emulate that client receive message about joining to topology earlier than some server nodes in topology.
 * And make this client connect to such servers.
 * To emulate this we connect client to second node in topology and pause sending message about joining finishing to
 * third node.
 */
public class IgniteClientConnectTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Latch to stop message sending. */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** Start client flag. */
    private final AtomicBoolean clientJustStarted = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestTcpDiscoverySpi disco = new TestTcpDiscoverySpi();

        if (igniteInstanceName.equals("client")) {
            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

            ipFinder.registerAddresses(Collections.singleton(new InetSocketAddress(InetAddress.getLoopbackAddress(), 47501)));

            disco.setIpFinder(ipFinder);
        }
        else
            disco.setIpFinder(ipFinder);

        disco.setJoinTimeout(2 * 60_000);
        disco.setSocketTimeout(1000);
        disco.setNetworkTimeout(2000);

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
    public void testClientConnectToBigTopology() throws Exception {
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

        clientCfg.setClientMode(true);

        clientJustStarted.set(true);

        IgniteEx client = startGrid(clientCfg);

        latch.countDown();

        System.err.println("GET ALL");
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

                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
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