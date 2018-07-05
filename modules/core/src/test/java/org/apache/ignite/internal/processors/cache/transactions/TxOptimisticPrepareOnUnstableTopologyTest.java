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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Tests optimistic prepare on unstable topology.
 */
public class TxOptimisticPrepareOnUnstableTopologyTest extends GridCommonAbstractTest {
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
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        ccfg.setBackups(2);
        ccfg.setCacheMode(PARTITIONED);

        c.setCacheConfiguration(ccfg);

        c.setClientMode(client);

        return c;
    }

    /**
     *
     */
    public void testPrepareOnUnstableTopology() throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            doPrepareOnUnstableTopology(4, false, isolation, 0);
            doPrepareOnUnstableTopology(4, true, isolation, 0);
            doPrepareOnUnstableTopology(4, false, isolation, TimeUnit.DAYS.toMillis(1));
            doPrepareOnUnstableTopology(4, true, isolation, TimeUnit.DAYS.toMillis(1));
        }
    }

    /**
     * @param keys Keys.
     * @param testClient Test client.
     * @param isolation Isolation.
     * @param timeout Timeout.
     */
    private void doPrepareOnUnstableTopology(int keys, boolean testClient, TransactionIsolation isolation,
        long timeout) throws Exception {
        Collection<Thread> threads = new ArrayList<>();

        try {
            // Start grid 1.
            IgniteEx grid1 = startGrid(0);

            assertFalse(grid1.configuration().isClientMode());

            threads.add(runCacheOperations(grid1, isolation, timeout, keys));

            TimeUnit.SECONDS.sleep(3L);

            client = testClient; // If test client start on node in client mode.

            // Start grid 2.
            IgniteEx grid2 = startGrid(1);

            assertEquals((Object)testClient, grid2.configuration().isClientMode());

            client = false;

            threads.add(runCacheOperations(grid2, isolation, timeout, keys));

            TimeUnit.SECONDS.sleep(3L);

            // Start grid 3.
            IgniteEx grid3 = startGrid(2);

            assertFalse(grid3.configuration().isClientMode());

            if (testClient)
                log.info("Started client node: " + grid3.name());

            threads.add(runCacheOperations(grid3, isolation, timeout, keys));

            TimeUnit.SECONDS.sleep(3L);

            // Start grid 4.
            IgniteEx grid4 = startGrid(3);

            assertFalse(grid4.configuration().isClientMode());

            threads.add(runCacheOperations(grid4, isolation, timeout, keys));

            TimeUnit.SECONDS.sleep(3L);

            stopThreads(threads);

            for (int i = 0; i < 4; i++) {
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
     * @param node Node.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param keys Number of keys.
     * @return Running thread.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private Thread runCacheOperations(Ignite node, TransactionIsolation isolation, long timeout, final int keys) {
        Thread t = new Thread() {
            @Override public void run() {
                while (run) {
                    TreeMap<Integer, String> vals = generateValues(keys);

                    try {
                        try (Transaction tx = node.transactions().txStart(TransactionConcurrency.OPTIMISTIC, isolation,
                            timeout, keys)){

                            IgniteCache<Object, Object> cache = node.cache(CACHE_NAME);

                            // Put or remove.
                            if (ThreadLocalRandom.current().nextDouble(1) < 0.65)
                                cache.putAll(vals);
                            else
                                cache.removeAll(vals.keySet());

                            tx.commit();
                        }
                        catch (Exception e) {
                            U.error(log(), "Failed cache operation.", e);
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
