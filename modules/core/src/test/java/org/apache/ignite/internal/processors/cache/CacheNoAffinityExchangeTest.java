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

package org.apache.ignite.internal.processors.cache;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class CacheNoAffinityExchangeTest extends GridCommonAbstractTest {

    private volatile boolean startClient;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (startClient)
            cfg.setClientMode(true);

        cfg.setDiscoverySpi(new TestDiscoverySpi());

        return cfg;
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    public void testNoAffinityChangeOnClientJoin() throws Exception {
        Ignite ig = startGrids(4);

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> atomicCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
                .setName("atomic").setAtomicityMode(CacheAtomicityMode.ATOMIC));

        IgniteCache<Integer, Integer> txCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
                .setName("tx").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        TestDiscoverySpi discoSpi = (TestDiscoverySpi) grid(2).context().discovery().getInjectedDiscoverySpi();

        CountDownLatch latch = new CountDownLatch(1);

        discoSpi.latch = latch;

        startClient = true;

        GridTestUtils.runAsync(() -> {
            try {
                startGrid(4);
            }
            catch (Exception ex) {
                throw new IgniteException(ex);
            }
        });

        U.sleep(5_000);

        assertEquals(new AffinityTopologyVersion(5, 0), grid(0).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(5, 0), grid(1).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(4, 3), grid(2).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(4, 3), grid(3).context().discovery().topologyVersionEx());

        for (int k = 0; k < 100; k++) {
            atomicCache.put(k, k);
            txCache.put(k, k);

            Lock lock = txCache.lock(k);
            lock.lock();
            lock.unlock();
        }

        for (int k = 0; k < 100; k++) {
            assertEquals(Integer.valueOf(k), atomicCache.get(k));
            assertEquals(Integer.valueOf(k), txCache.get(k));
        }

        assertEquals(new AffinityTopologyVersion(5, 0), grid(0).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(5, 0), grid(1).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(4, 3), grid(2).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(4, 3), grid(3).context().discovery().topologyVersionEx());

        latch.countDown();
    }

    public void testNoAffinityChangeOnClientLeft() throws Exception {
        Ignite ig = startGrids(4);

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> atomicCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("atomic").setAtomicityMode(CacheAtomicityMode.ATOMIC));

        IgniteCache<Integer, Integer> txCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("tx").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        startClient = true;

        startGrid(4);

        TestDiscoverySpi discoSpi = (TestDiscoverySpi)grid(2).context().discovery().getInjectedDiscoverySpi();

        CountDownLatch latch = new CountDownLatch(1);

        discoSpi.latch = latch;

        GridTestUtils.runAsync(() -> {
            try {
                stopGrid(4);
            }
            catch (Exception ex) {
                throw new IgniteException(ex);
            }
        });

        U.sleep(5_000);

        assertEquals(new AffinityTopologyVersion(6, 0), grid(0).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(6, 0), grid(1).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(5, 0), grid(2).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(5, 0), grid(3).context().discovery().topologyVersionEx());

        for (int k = 0; k < 100; k++) {
            atomicCache.put(k, k);
            txCache.put(k, k);

            Lock lock = txCache.lock(k);
            lock.lock();
            lock.unlock();
        }

        for (int k = 0; k < 100; k++) {
            assertEquals(Integer.valueOf(k), atomicCache.get(k));
            assertEquals(Integer.valueOf(k), txCache.get(k));
        }

        assertEquals(new AffinityTopologyVersion(6, 0), grid(0).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(6, 0), grid(1).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(5, 0), grid(2).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(5, 0), grid(3).context().discovery().topologyVersionEx());

        latch.countDown();
    }

    public void testClientsRestart() throws Exception {
        Ignite ig = startGrids(4);

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> atomicCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("atomic").setAtomicityMode(CacheAtomicityMode.ATOMIC));

        IgniteCache<Integer, Integer> txCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("tx").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        startClient = true;

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            try {
                for (int i = 0; i < 10; i++)
                    startGrid(4 + i);
                for (int i = 9; i >= 0; i--)
                    stopGrid(4 + i);
            }
            catch (Exception ex) {
                throw new IgniteException(ex);
            }

            stop.set(true);
        });

        Random rnd = new Random();

        int ops = 0;

        while (!stop.get()) {
            int key = rnd.nextInt();

            atomicCache.put(key, key);
            txCache.put(key, key);

            ops++;
        }

        fut.get();
    }

    public static class TestDiscoverySpi extends TcpDiscoverySpi {
        private volatile CountDownLatch latch;

        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage || msg instanceof TcpDiscoveryNodeLeftMessage || msg instanceof TcpDiscoveryNodeFailedMessage) {
                CountDownLatch latch0 = latch;

                if (latch0 != null)
                    try {
                        latch0.await();
                    }
                    catch (InterruptedException ex) {
                        throw new IgniteException(ex);
                    }
            }

            super.startMessageProcess(msg);
        }
    }

}
