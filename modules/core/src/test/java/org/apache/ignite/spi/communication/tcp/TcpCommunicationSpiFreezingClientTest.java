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

package org.apache.ignite.spi.communication.tcp;

import java.lang.management.ManagementFactory;
import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests that freezing due to JVM STW client will be failed if connection can't be established.
 */
public class TcpCommunicationSpiFreezingClientTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(120000);
        cfg.setClientFailureDetectionTimeout(120000);

        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        spi.setConnectTimeout(3000);
        spi.setMaxConnectTimeout(6000);
        spi.setReconnectCount(3);
        spi.setIdleConnectionTimeout(100);
        spi.setSharedMemoryPort(-1);

        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(spi);
        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).setWriteSynchronizationMode(FULL_SYNC).
            setCacheMode(PARTITIONED).setAtomicityMode(ATOMIC));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL);
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFreezingClient() throws Exception {
        try {
            final IgniteEx srv = startGrids(2);

            final IgniteEx client = startClientGrid("client");

            final int keysCnt = 100_000;

            try (IgniteDataStreamer<Integer, byte[]> streamer = srv.dataStreamer(DEFAULT_CACHE_NAME)) {
                for (int i = 0; i < keysCnt; i++)
                    streamer.addData(i, new byte[512]);
            }

            // Wait for connections go idle.
            doSleep(1000);

            srv.compute(srv.cluster().forNode(client.localNode())).withNoFailover().call(new ClientClosure());

            fail("Client node must be kicked from topology");
        }
        catch (ClusterTopologyException e) {
            // Expected.

            e.printStackTrace();

            System.out.println(e);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    public static class ClientClosure implements IgniteCallable<Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        @IgniteInstanceResource
        private transient Ignite ignite;

        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            Thread loadThread = new Thread() {
                @Override public void run() {
                    log.info("result = " + simulateLoad());
                }
            };

            loadThread.setName("load-thread");
            loadThread.start();

            int cnt = 0;

            final Iterator<Cache.Entry<Integer, byte[]>> it = ignite.cache(DEFAULT_CACHE_NAME).
                query(new ScanQuery<Integer, byte[]>().setPageSize(100000)).iterator();

            while (it.hasNext()) {
                Cache.Entry<Integer, byte[]> entry = it.next();

                // Trigger STW.
                final long[] tids = ManagementFactory.getThreadMXBean().findDeadlockedThreads();

                cnt++;
            }

            loadThread.join();

            return cnt;
        }

        /**
         *
         */
        public static double simulateLoad() {
            double d = 0;

            for (int i = 0; i < 1000000000; i++)
                d += Math.log(Math.PI * i);

            return d;
        }
    }
}
