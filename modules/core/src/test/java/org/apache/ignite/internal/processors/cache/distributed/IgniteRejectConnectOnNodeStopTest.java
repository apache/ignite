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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Sanity test to check that node starts to reject connections when stop procedure started.
 */
public class IgniteRejectConnectOnNodeStopTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private static CountDownLatch stopLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TestDiscoverySpi());

        TcpDiscoverySpi discoSpi = ((TcpDiscoverySpi)cfg.getDiscoverySpi());

        discoSpi.setReconnectCount(2);
        discoSpi.setAckTimeout(30_000);
        discoSpi.setSocketTimeout(30_000);
        discoSpi.setIpFinder(IP_FINDER);

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)cfg.getCommunicationSpi();

        commSpi.setConnectTimeout(600000);
        commSpi.setMaxConnectTimeout(600000 * 10);
        commSpi.setReconnectCount(100);
        commSpi.setSocketWriteTimeout(600000);
        commSpi.setAckSendThreshold(100);

        cfg.setClientMode(client);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeStop() throws Exception {
        Ignite srv = startGrid(0);

        client = true;

        final Ignite c = startGrid(1);

        ClusterGroup grp = srv.cluster().forClients();

        IgniteCompute srvCompute = srv.compute(grp);

        srvCompute.call(new DummyClosure());

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                IgniteCache cache = c.cache(DEFAULT_CACHE_NAME);

                for (int i = 0; i < 100_000; i++) {
                    try {
                        cache.put(1, 1);
                    }
                    catch (Exception ignore) {
                        break;
                    }
                }
            }
        }, "cache-put");

        U.sleep(100);

        final CountDownLatch stopStartLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                stopStartLatch.countDown();

                c.close();
            }
        });

        boolean err = false;

        try{
            stopStartLatch.await();

            IgniteCacheMessageRecoveryAbstractTest.closeSessions(srv);

            long stopTime = U.currentTimeMillis() + 10_000;

            while (U.currentTimeMillis() < stopTime) {
                try {
                    srvCompute.call(new DummyClosure());
                }
                catch (ClusterTopologyException e) {
                    err = true;

                    assertFalse(fut2.isDone());

                    break;
                }
            }
        }
        finally {
            stopLatch.countDown();
        }

        fut.get();
        fut2.get();

        assertTrue("Failed to get excpected error", err);
    }


    /**
     *
     */
    public static class DummyClosure implements IgniteCallable<Object> {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return 1;
        }
    }

    /**
     *
     */
    static class TestDiscoverySpi extends TcpDiscoverySpi {
        @Override public void spiStop() throws IgniteSpiException {
            // Called communication SPI onContextDestroyed, but do not allow discovery to stop.
            if (ignite.configuration().isClientMode()) {
                try {
                    stopLatch.await(1, MINUTES);
                }
                catch (InterruptedException ignore) {
                    // No-op.
                }
            }

            super.spiStop();
        }
    }
}
