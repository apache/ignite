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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Tests explicit lock.
 */
public class IgniteCacheMultiTxLockSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "part_cache";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private volatile boolean run = true;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        assertEquals(0, G.allGrids().size());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        ccfg.setBackups(2);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setStartSize(100000);

        LruEvictionPolicy plc = new LruEvictionPolicy();
        plc.setMaxSize(100000);

        ccfg.setEvictionPolicy(plc);

        c.setCacheConfiguration(ccfg);

        c.setClientMode(client);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitLockOneKey() throws Exception {
        checkExplicitLock(1, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitLockManyKeys() throws Exception {
        checkExplicitLock(4, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitLockManyKeysWithClient() throws Exception {
        checkExplicitLock(4, true);
    }

    /**
     * @param keys Number of keys.
     * @param testClient If {@code true} uses one client node.
     * @throws Exception If failed.
     */
    public void checkExplicitLock(int keys, boolean testClient) throws Exception {
        Collection<Thread> threads = new ArrayList<>();

        try {
            // Start grid 1.
            IgniteEx grid1 = startGrid(1);

            assertFalse(grid1.configuration().isClientMode());

            threads.add(runCacheOperations(grid1.cachex(CACHE_NAME), keys));

            TimeUnit.SECONDS.sleep(3L);

            client = testClient; // If test client start on node in client mode.

            // Start grid 2.
            IgniteEx grid2 = startGrid(2);

            assertEquals((Object)testClient, grid2.configuration().isClientMode());

            client = false;

            threads.add(runCacheOperations(grid2.cachex(CACHE_NAME), keys));

            TimeUnit.SECONDS.sleep(3L);

            // Start grid 3.
            IgniteEx grid3 = startGrid(3);

            assertFalse(grid3.configuration().isClientMode());

            if (testClient)
                log.info("Started client node: " + grid3.name());

            threads.add(runCacheOperations(grid3.cachex(CACHE_NAME), keys));

            TimeUnit.SECONDS.sleep(3L);

            // Start grid 4.
            IgniteEx grid4 = startGrid(4);

            assertFalse(grid4.configuration().isClientMode());

            threads.add(runCacheOperations(grid4.cachex(CACHE_NAME), keys));

            TimeUnit.SECONDS.sleep(3L);

            stopThreads(threads);

            for (int i = 1; i <= 4; i++) {
                IgniteTxManager tm = ((IgniteKernal)grid(i)).internalCache(CACHE_NAME).context().tm();

                assertEquals("txMap is not empty:" + i, 0, tm.idMapSize());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param threads Thread which will be stopped.
     */
    private void stopThreads(Iterable<Thread> threads) {
        try {
            run = false;

            for (Thread thread : threads)
                thread.join();
        }
        catch (Exception e) {
            U.error(log(), "Couldn't stop threads.", e);
        }
    }

    /**
     * @param cache Cache.
     * @param keys Number of keys.
     * @return Running thread.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private Thread runCacheOperations(final IgniteInternalCache<Object,Object> cache, final int keys) {
        Thread t = new Thread() {
            @Override public void run() {
                while (run) {
                    TreeMap<Integer, String> vals = generateValues(keys);

                    try {
                        // Explicit lock.
                        cache.lock(vals.firstKey(), 0);

                        try {
                            // Put or remove.
                            if (ThreadLocalRandom.current().nextDouble(1) < 0.65)
                                cache.putAll(vals);
                            else
                                cache.removeAll(vals.keySet());
                        }
                        catch (Exception e) {
                            U.error(log(), "Failed cache operation.", e);
                        }
                        finally {
                            cache.unlock(vals.firstKey());
                        }

                        U.sleep(100);
                    }
                    catch (Exception e){
                        U.error(log(), "Failed unlock.", e);
                    }
                }
            }
        };

        t.start();

        return t;
    }

    /**
     * @param cnt Number of keys to generate.
     * @return Map.
     */
    private TreeMap<Integer, String> generateValues(int cnt) {
        TreeMap<Integer, String> res = new TreeMap<>();

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (res.size() < cnt) {
            int key = rnd.nextInt(0, 100);

            res.put(key, String.valueOf(key));
        }

        return res;
    }
}