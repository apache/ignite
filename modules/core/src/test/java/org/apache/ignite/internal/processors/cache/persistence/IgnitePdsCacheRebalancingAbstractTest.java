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

package org.apache.ignite.internal.processors.cache.persistence;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 * Test for rebalancing and persistence integration.
 */
public abstract class IgnitePdsCacheRebalancingAbstractTest extends IgnitePdsCacheRebalancingCommonAbstractTest {
    /**
     * Test that outdated partitions on restarted nodes are correctly replaced with newer versions.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testRebalancingOnRestart() throws Exception {
        Ignite ignite0 = startGrid(0);

        startGrid(1);

        IgniteEx ignite2 = startGrid(2);

        ignite0.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1 = ignite0.cache(CACHE);

        for (int i = 0; i < 5000; i++)
            cache1.put(i, i);

        ignite2.close();

        awaitPartitionMapExchange();

        ignite0.resetLostPartitions(Collections.singletonList(cache1.getName()));

        assert cache1.lostPartitions().isEmpty();

        for (int i = 0; i < 5000; i++)
            cache1.put(i, i * 2);

        info(">>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>");

        info(">>> Done puts...");

        ignite2 = startGrid(2);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache3 = ignite2.cache(CACHE);

        for (int i = 0; i < 100; i++)
            assertEquals(String.valueOf(i), (Integer)(i * 2), cache3.get(i));
    }

    /**
     * Test that outdated partitions on restarted nodes are correctly replaced with newer versions.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testRebalancingOnRestartAfterCheckpoint() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        IgniteEx ignite1 = startGrid(1);

        IgniteEx ignite2 = startGrid(2);
        IgniteEx ignite3 = startGrid(3);

        ignite0.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1 = ignite0.cache(CACHE);

        for (int i = 0; i < 1000; i++)
            cache1.put(i, i);

        forceCheckpoint(ignite0);
        forceCheckpoint(ignite1);

        info("++++++++++ After checkpoint");

        ignite2.close();
        ignite3.close();

        ignite0.resetLostPartitions(Collections.singletonList(cache1.getName()));

        assert cache1.lostPartitions().isEmpty();

        for (int i = 0; i < 1000; i++)
            cache1.put(i, i * 2);

        info(">>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>");

        info(">>> Done puts...");

        startGrid(2);
        startGrid(3);

        awaitPartitionMapExchange();

        ignite2 = grid(2);
        ignite3 = grid(3);

        IgniteCache<Integer, Integer> cache2 = ignite2.cache(CACHE);
        IgniteCache<Integer, Integer> cache3 = ignite3.cache(CACHE);

        for (int i = 0; i < 100; i++) {
            assertEquals(String.valueOf(i), (Integer)(i * 2), cache2.get(i));
            assertEquals(String.valueOf(i), (Integer)(i * 2), cache3.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTopologyChangesWithConstantLoad() throws Exception {
        final long timeOut = U.currentTimeMillis() + 5 * 60 * 1000;

        final int entriesCnt = 10_000;
        final int maxNodesCnt = 4;
        final int topChanges = 5;//SF.applyLB(15, 5);
        final boolean allowRemoves = true;

        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicBoolean suspend = new AtomicBoolean();
        final AtomicBoolean suspended = new AtomicBoolean();

        final ConcurrentMap<Integer, TestValue> map = new ConcurrentHashMap<>();

        Ignite ignite = startGridsMultiThreaded(4);

        ignite.cluster().active(true);

        try (IgniteDataStreamer<Integer, TestValue> ds = ignite.dataStreamer(INDEXED_CACHE)) {
            for (int i = 0; i < entriesCnt; i++) {
                ds.addData(i, new TestValue(i, i, i));
                map.put(i, new TestValue(i, i, i));
            }
        }

        IgniteCache<Integer, TestValue> cache = ignite.cache(INDEXED_CACHE);

        final AtomicLong orderCounter = new AtomicLong(entriesCnt);

        final AtomicInteger nodesCnt = new AtomicInteger(4);

        IgniteInternalFuture fut = runMultiThreadedAsync(new Callable<Void>() {
            /**
             * @param chance Chance of remove operation in percents.
             * @return {@code true} if it should be remove operation.
             */
            private boolean removeOp(int chance) {
                return ThreadLocalRandom.current().nextInt(100) + 1 <= chance;
            }

            @Override public Void call() throws Exception {
                Random rnd = ThreadLocalRandom.current();

                while (true) {
                    if (stop.get())
                        return null;

                    if (suspend.get()) {
                        suspended.set(true);

                        U.sleep(10);

                        continue;
                    }

                    int k = rnd.nextInt(entriesCnt);
                    long order = orderCounter.get();

                    int v1 = 0, v2 = 0;
                    boolean remove = false;

                    if (removeOp(allowRemoves ? 20 : 0))
                        remove = true;
                    else {
                        v1 = rnd.nextInt();
                        v2 = rnd.nextInt();
                    }

                    int nodes = nodesCnt.get();

                    Ignite ignite;

                    try {
                        ignite = grid(rnd.nextInt(nodes));
                    }
                    catch (Exception ignored) {
                        continue;
                    }

                    Transaction tx = null;
                    boolean success = true;

                    if (explicitTx)
                        tx = ignite.transactions().txStart();

                    try {
                        IgniteCache<Object, Object> cache = ignite.cache(INDEXED_CACHE);

                        if (remove)
                            cache.remove(k);
                        else
                            cache.put(k, new TestValue(order, v1, v2));
                    }
                    catch (Exception ignored) {
                        success = false;
                    }
                    finally {
                        if (tx != null) {
                            try {
                                tx.commit();
                            }
                            catch (Exception ignored) {
                                success = false;
                            }
                        }
                    }

                    if (success) {
                        map.put(k, new TestValue(order, v1, v2, remove));

                        orderCounter.incrementAndGet();
                    }
                }
            }
        }, 1, "load-runner");

        // "False" means stop last started node, "True" - start new node.
        List<Boolean> predefinedChanges = Lists.newArrayList(false, false, true, true);

        List<Boolean> topChangesHistory = new ArrayList<>();

        try {
            for (int it = 0; it < topChanges; it++) {
                if (U.currentTimeMillis() > timeOut)
                    break;

                U.sleep(SF.applyLB(3_000, 500));

                boolean addNode;

                if (it < predefinedChanges.size())
                    addNode = predefinedChanges.get(it);
                else if (nodesCnt.get() <= maxNodesCnt / 2)
                    addNode = true;
                else if (nodesCnt.get() >= maxNodesCnt)
                    addNode = false;
                else // More chance that node will be added
                    addNode = ThreadLocalRandom.current().nextInt(3) <= 1;

                if (addNode)
                    startGrid(nodesCnt.getAndIncrement());
                else
                    stopGrid(nodesCnt.decrementAndGet());

                topChangesHistory.add(addNode);

                awaitPartitionMapExchange();

                if (fut.error() != null)
                    break;

                // Suspend loader and wait for last operation completion.
                suspend.set(true);
                GridTestUtils.waitForCondition(suspended::get, 5_000);

                // Fix last successful cache operation to skip operations that can be performed during check.
                long maxOrder = orderCounter.get();

                for (Map.Entry<Integer, TestValue> entry : map.entrySet()) {
                    final String assertMsg = "Iteration: " + it + ". Changes: " + Objects.toString(topChangesHistory)
                            + ". Key: " + Integer.toString(entry.getKey());

                    TestValue expected = entry.getValue();

                    if (expected.order < maxOrder)
                        continue;

                    TestValue actual = cache.get(entry.getKey());

                    if (expected.removed) {
                        assertNull(assertMsg + " should be removed.", actual);

                        continue;
                    }

                    if (entry.getValue().order < maxOrder)
                        continue;

                    assertEquals(assertMsg, expected, actual);
                }

                // Resume progress for loader.
                suspend.set(false);
                suspended.set(false);
            }
        }
        finally {
            stop.set(true);
        }

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testForceRebalance() throws Exception {
        testForceRebalance(CACHE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testForceRebalanceClientTopology() throws Exception {
        filteredCacheEnabled = true;

        try {
            testForceRebalance(FILTERED_CACHE);
        }
        finally {
            filteredCacheEnabled = false;
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void testForceRebalance(String cacheName) throws Exception {
        startGrids(4);

        final Ignite ig = grid(1);

        ig.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> c = ig.cache(cacheName);

        Integer val = 0;

        for (int i = 0; i < 5; i++) {
            info("Iteration: " + i);

            Integer key = primaryKey(ignite(3).cache(cacheName));

            c.put(key, val);

            stopGrid(3);

            val++;

            c.put(key, val);

            assertEquals(val, c.get(key));

            startGrid(3);

            awaitPartitionMapExchange();

            Object val0 = ignite(3).cache(cacheName).get(key);

            assertEquals(val, val0);
        }
    }

    /**
     * @throws Exception If failed
     */
    @Test
    public void testPartitionCounterConsistencyOnUnstableTopology() throws Exception {
        Ignite ig = startGridsMultiThreaded(4);

        ig.cluster().active(true);

        int keys = 0;

        try (IgniteDataStreamer<Object, Object> ds = ig.dataStreamer(CACHE)) {
            ds.allowOverwrite(true);

            for (; keys < 10_000; keys++)
                ds.addData(keys, keys);
        }

        assertPartitionsSame(idleVerify(grid(0), CACHE));

        for (int it = 0; it < SF.applyLB(10, 3); it++) {
            final int it0 = it;

            IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                try {
                    int dataLoadTimeout = SF.applyLB(500, 250);

                    stopGrid(3);

                    U.sleep(dataLoadTimeout); // Wait for data load.

                    startGrid(3);

                    U.sleep(dataLoadTimeout); // Wait for data load.

                    if (it0 % 2 != 0) {
                        stopGrid(2);

                        U.sleep(dataLoadTimeout); // Wait for data load.

                        startGrid(2);
                    }

                    awaitPartitionMapExchange();
                }
                catch (Exception e) {
                    error("Unable to start/stop grid", e);

                    throw new RuntimeException(e);
                }
            });

            IgniteCache<Object, Object> cache = ig.cache(CACHE);

            while (!fut.isDone()) {
                int nextKeys = keys + 10;

                for (;keys < nextKeys; keys++)
                    cache.put(keys, keys);
            }

            fut.get();

            log.info("Checking data...");

            Map<Integer, Long> cntrs = new HashMap<>();

            for (int g = 0; g < 4; g++) {
                IgniteEx ig0 = grid(g);

                for (GridDhtLocalPartition part : ig0.cachex(CACHE).context().topology().currentLocalPartitions()) {
                    if (cntrs.containsKey(part.id()))
                        assertEquals(String.valueOf(part.id()), (long) cntrs.get(part.id()), part.updateCounter());
                    else
                        cntrs.put(part.id(), part.updateCounter());
                }

                IgniteCache<Integer, String> ig0cache = ig0.cache(CACHE);

                for (Cache.Entry<Integer, String> entry : ig0cache.query(new ScanQuery<Integer, String>()))
                    assertEquals(entry.getKey() + " " + g, entry.getKey(), entry.getValue());
            }

            assertEquals(ig.affinity(CACHE).partitions(), cntrs.size());
        }
    }

    /**
     * Test rebalancing of in-memory cache on the node with mixed data region configurations.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalancingWithMixedDataRegionConfigurations() throws Exception {
        int entriesCount = 10_000;

        Ignite ignite0 = startGrids(2);

        ignite0.cluster().active(true);

        IgniteCache<Integer, TestValue> cachePds = ignite0.cache(INDEXED_CACHE);
        IgniteCache<Integer, TestValue> cacheInMem = ignite0.cache(INDEXED_CACHE_IN_MEMORY);

        for (int i = 0; i < entriesCount / 2; i++) {
            TestValue value = new TestValue(i, i * 2, i * 3);

            cachePds.put(i, value);
            cacheInMem.put(i, value);
        }

        forceCheckpoint();

        stopGrid(1);

        for (int i = entriesCount / 2; i < entriesCount; i++) {
            TestValue value = new TestValue(i, i * 2, i * 3);

            cachePds.put(i, value);
            cacheInMem.put(i, value);
        }

        IgniteEx ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        IgniteInternalCache<Integer, TestValue> cachePds1 = ignite1.cachex(INDEXED_CACHE);
        IgniteInternalCache<Integer, TestValue> cacheInMem1 = ignite1.cachex(INDEXED_CACHE_IN_MEMORY);

        CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};

        assertEquals(entriesCount, cachePds1.localSize(peekAll));
        assertEquals(entriesCount, cacheInMem1.localSize(peekAll));

        for (int i = 0; i < entriesCount; i++) {
            TestValue value = new TestValue(i, i * 2, i * 3);

            assertEquals(value, cachePds1.localPeek(i, peekAll));
            assertEquals(value, cacheInMem1.localPeek(i, peekAll));
        }
    }
}
