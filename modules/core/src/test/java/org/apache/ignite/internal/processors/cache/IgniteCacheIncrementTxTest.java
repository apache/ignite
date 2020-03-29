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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheIncrementTxTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);

        startClientGrid(SRVS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIncrementTxTopologyChange0() throws Exception {
        nodeJoin(cacheConfiguration(0));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIncrementTxTopologyChange1() throws Exception {
        nodeJoin(cacheConfiguration(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIncrementTxTopologyChange2() throws Exception {
        nodeJoin(cacheConfiguration(2));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void nodeJoin(CacheConfiguration ccfg) throws Exception {
        ignite(0).createCache(ccfg);

        try {
            final Map<Integer, AtomicInteger> incMap = new LinkedHashMap<>();

            final int KEYS = 10;

            for (int i = 0; i < KEYS; i++)
                incMap.put(i, new AtomicInteger());

            final int NODES = SRVS + 1;

            final int START_NODES = 5;

            final AtomicInteger nodeIdx = new AtomicInteger(NODES);

            final IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int node = nodeIdx.getAndIncrement();

                    Ignite ignite = startGrid(node);

                    IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

                    for (int i = 0; i < 1000; i++)
                        incrementTx(ignite, cache, incMap);

                    return null;
                }
            }, START_NODES, "start-thread");

            IgniteInternalFuture<?> txFut = updateFuture(NODES, incMap, fut);

            fut.get();
            txFut.get();

            log.info("First updates: " + incMap);

            checkCache(NODES + START_NODES, incMap);

            if (ccfg.getBackups() > 0) {
                for (int i = 0; i < START_NODES; i++) {
                    final int stopIdx = NODES + i;

                    IgniteInternalFuture<?> stopFut = GridTestUtils.runAsync(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            U.sleep(500);

                            stopGrid(stopIdx);

                            return null;
                        }
                    }, "stop-thread");

                    txFut = updateFuture(NODES, incMap, stopFut);

                    stopFut.get();
                    txFut.get();

                    checkCache(NODES + START_NODES - (i + 1), incMap);

                    for (int n = 0; n < SRVS; n++)
                        ignite(n).cache(DEFAULT_CACHE_NAME).rebalance().get();
                }
            }
            else {
                for (int i = 0; i < START_NODES; i++)
                    stopGrid(NODES + i);

                return;
            }

            log.info("Second updates: " + incMap);

            checkCache(NODES, incMap);
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param expNodes Expected nodes number.
     * @param incMap Increments map.
     */
    private void checkCache(int expNodes, Map<Integer, AtomicInteger> incMap) {
        List<Ignite> nodes = G.allGrids();

        assertEquals(expNodes, nodes.size());

        for (Ignite node : nodes) {
            IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

            for (Map.Entry<Integer, AtomicInteger> e : incMap.entrySet())
                assertEquals((Integer)e.getValue().get(), cache.get(e.getKey()));
        }
    }

    /**
     * @param nodes Number of nodes.
     * @param incMap Increments map.
     * @param fut Future to wait for.
     * @return Future.
     */
    private IgniteInternalFuture<?> updateFuture(final int nodes,
        final Map<Integer, AtomicInteger> incMap,
        final IgniteInternalFuture<?> fut) {
        final AtomicInteger threadIdx = new AtomicInteger(0);

        return GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int node = threadIdx.incrementAndGet() % nodes;

                Ignite ignite = grid(node);

                Thread.currentThread().setName("update-" + ignite.name());

                IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

                while (!fut.isDone())
                    incrementTx(ignite, cache, incMap);

                for (int i = 0; i < 50; i++)
                    incrementTx(ignite, cache, incMap);

                return null;
            }
        }, nodes * 3, "update-thread");
    }

    /**
     * @param ignite Node.
     * @param cache Cache.
     * @param incMap Increments map.
     */
    private void incrementTx(Ignite ignite, IgniteCache<Integer, Integer> cache, Map<Integer, AtomicInteger> incMap) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        boolean singleKey = rnd.nextBoolean();

        List<Integer> keys = new ArrayList<>(incMap.size());

        try {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                for (Integer key : incMap.keySet()) {
                    if (singleKey || rnd.nextBoolean()) {
                        Integer val = cache.get(key);

                        if (val == null)
                            val = 1;
                        else
                            val = val + 1;

                        cache.put(key, val);

                        keys.add(key);
                    }

                    if (singleKey)
                        break;
                }

                tx.commit();

                for (Integer key : keys)
                    incMap.get(key).incrementAndGet();
            }
        }
        catch (Exception e) {
            log.info("Tx failed: " + e);
        }
    }

    /**
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(int backups) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(backups);

        return ccfg;
    }
}
