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

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheCreatePutTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        MemoryConfiguration mCfg = new MemoryConfiguration();

        mCfg.setPageSize(1024 /* 1Kb */);

        cfg.setMemoryConfiguration(mCfg);

        OptimizedMarshaller marsh = new OptimizedMarshaller();
        marsh.setRequireSerializable(false);

        cfg.setMarshaller(marsh);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName("cache*");
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(32, null));

        cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(client);
        cfg.setPeerClassLoadingEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000L;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartNodes() throws Exception {
        Ignite ignite = startGrid(0);

        IgniteCache<Integer, TestObject> cache = ignite.getOrCreateCache("cache1");

        cache.put(1, new TestObject(1024));

        TestObject actual = cache.get(1);

        assertEquals(new TestObject(1024), actual);

//        for (int iter = 0; iter < 5; iter++) {
//            for (int i = 0; i < 100_000; i++)
//                cache.put(i, new TestObject(i % 1000));
//
//            for (int i = 0; i < 100_000; i++) {
//                TestObject actual = cache.get(i);
//
//                assertEquals(new TestObject(i % 1000), actual);
//            }
//        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdatesAndCacheStart() throws Exception {
        final int NODES = 4;

        startGridsMultiThreaded(NODES);

        Ignite ignite0 = ignite(0);

        ignite0.createCache(cacheConfiguration("atomic-cache", ATOMIC));
        ignite0.createCache(cacheConfiguration("tx-cache", TRANSACTIONAL));

        final long stopTime = System.currentTimeMillis() + 60_000;

        final AtomicInteger updateThreadIdx = new AtomicInteger();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int nodeIdx = updateThreadIdx.getAndIncrement() % NODES;

                Ignite node = ignite(nodeIdx);

                IgniteCache cache1 = node.cache("atomic-cache");
                IgniteCache cache2 = node.cache("tx-cache");

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int iter = 0;

                while (System.currentTimeMillis() < stopTime) {
                    Integer key = rnd.nextInt(10_000);

                    cache1.put(key, key);

                    cache2.put(key, key);

                    if (iter++ % 1000 == 0)
                        log.info("Update iteration: " + iter);
                }

                return null;
            }
        }, NODES * 2, "update-thread");

        final AtomicInteger cacheThreadIdx = new AtomicInteger();

        IgniteInternalFuture<?> cacheFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int nodeIdx = cacheThreadIdx.getAndIncrement() % NODES;

                Ignite node = ignite(nodeIdx);

                int iter = 0;

                while (System.currentTimeMillis() < stopTime) {
                    String cacheName = "dynamic-cache-" + nodeIdx;

                    CacheConfiguration ccfg = new CacheConfiguration();

                    ccfg.setName(cacheName);

                    node.createCache(ccfg);

                    node.destroyCache(cacheName);

                    U.sleep(500);

                    if (iter++ % 1000 == 0)
                        log.info("Cache create iteration: " + iter);
                }

                return null;
            }
        }, NODES, "cache-thread");

        while (!fut.isDone()) {
            client = true;

            startGrid(NODES);

            stopGrid(NODES);

            client = false;

            startGrid(NODES);

            stopGrid(NODES);
        }

        fut.get();
        cacheFut.get();
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setCacheMode(REPLICATED);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /**
     *
     */
    private static class TestObject {
        /** */
        byte[] val;

        /**
         * @param val Value.
         */
        public TestObject(int val) {
            this.val = new byte[val];
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject object = (TestObject)o;

            return Arrays.equals(val, object.val);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Arrays.hashCode(val);
        }
    }
}