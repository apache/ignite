/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import com.google.common.collect.Lists;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 * Test for rebalancing and persistence integration.
 */
public abstract class IgnitePdsCacheRebalancingAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Default cache. */
    private static final String CACHE = "cache";

    /** Cache with node filter. */
    private static final String FILTERED_CACHE = "filtered";

    /** Cache with enabled indexes. */
    private static final String INDEXED_CACHE = "indexed";

    /** */
    protected boolean explicitTx;

    /** Set to enable filtered cache on topology. */
    private boolean filteredCacheEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setRebalanceThreadPoolSize(2);

        CacheConfiguration ccfg1 = cacheConfiguration(CACHE)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
            .setBackups(2)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setIndexedTypes(Integer.class, Integer.class)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setRebalanceBatchesPrefetchCount(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        CacheConfiguration ccfg2 = cacheConfiguration(INDEXED_CACHE)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        QueryEntity qryEntity = new QueryEntity(Integer.class.getName(), TestValue.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("v1", Integer.class.getName());
        fields.put("v2", Integer.class.getName());

        qryEntity.setFields(fields);

        QueryIndex qryIdx = new QueryIndex("v1", true);

        qryEntity.setIndexes(Collections.singleton(qryIdx));

        ccfg2.setQueryEntities(Collections.singleton(qryEntity));

        List<CacheConfiguration> cacheCfgs = new ArrayList<>();
        cacheCfgs.add(ccfg1);
        cacheCfgs.add(ccfg2);

        if (filteredCacheEnabled && !gridName.endsWith("0")) {
            CacheConfiguration ccfg3 = cacheConfiguration(FILTERED_CACHE)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
                .setBackups(2)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setNodeFilter(new CoordinatorNodeFilter());

            cacheCfgs.add(ccfg3);
        }

        cfg.setCacheConfiguration(asArray(cacheCfgs));

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4)
            .setCheckpointFrequency(checkpointFrequency())
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setName("dfltDataRegion")
                .setPersistenceEnabled(true)
                .setMaxSize(512 * 1024 * 1024)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(IP_FINDER)
        );

        return cfg;
    }

    /**
     * @param cacheCfgs Cache cfgs.
     */
    private static CacheConfiguration[] asArray(List<CacheConfiguration> cacheCfgs) {
        CacheConfiguration[] res = new CacheConfiguration[cacheCfgs.size()];
        for (int i = 0; i < res.length; i++)
            res[i] = cacheCfgs.get(i);

        return res;
    }

    /**
     * @return Checkpoint frequency;
     */
    protected long checkpointFrequency() {
        return DataStorageConfiguration.DFLT_CHECKPOINT_FREQ;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected long getPartitionMapExchangeTimeout() {
        return 60 * 1000;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected abstract CacheConfiguration cacheConfiguration(String cacheName);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test that outdated partitions on restarted nodes are correctly replaced with newer versions.
     *
     * @throws Exception If fails.
     */
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
    public void testTopologyChangesWithConstantLoad() throws Exception {
        final long timeOut = U.currentTimeMillis() + 5 * 60 * 1000;

        final int entriesCnt = 10_000;
        final int maxNodesCnt = 4;
        final int topChanges = 25;
        final boolean allowRemoves = true;

        final AtomicLong orderCounter = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicBoolean suspend = new AtomicBoolean();
        final AtomicBoolean suspended = new AtomicBoolean();

        final ConcurrentMap<Integer, TestValue> map = new ConcurrentHashMap<>();

        Ignite ignite = startGridsMultiThreaded(4);

        ignite.cluster().active(true);

        IgniteCache<Integer, TestValue> cache = ignite.cache(INDEXED_CACHE);

        for (int i = 0; i < entriesCnt; i++) {
            long order = orderCounter.get();

            cache.put(i, new TestValue(order, i, i));
            map.put(i, new TestValue(order, i, i));

            orderCounter.incrementAndGet();
        }

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

                U.sleep(3_000);

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
    public void testForceRebalance() throws Exception {
        testForceRebalance(CACHE);
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testPartitionCounterConsistencyOnUnstableTopology() throws Exception {
        final Ignite ig = startGrids(4);

        ig.cluster().active(true);

        int keys = 0;

        try (IgniteDataStreamer<Object, Object> ds = ig.dataStreamer(CACHE)) {
            ds.allowOverwrite(true);

            for (; keys < 10_000; keys++)
                ds.addData(keys, keys);
        }

        for (int it = 0; it < 10; it++) {
            final int it0 = it;

            IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                try {
                    stopGrid(3);

                    U.sleep(500); // Wait for data load.

                    startGrid(3);

                    U.sleep(500); // Wait for data load.

                    if (it0 % 2 != 0) {
                        stopGrid(2);

                        U.sleep(500); // Wait for data load.

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

                for (int k0 = 0; k0 < keys; k0++)
                    assertEquals(String.valueOf(k0) + " " + g, k0, ig0.cache(CACHE).get(k0));
            }

            assertEquals(ig.affinity(CACHE).partitions(), cntrs.size());
        }
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** Operation order. */
        private final long order;

        /** V 1. */
        private final int v1;

        /** V 2. */
        private final int v2;

        /** Flag indicates that value has removed. */
        private final boolean removed;

        private TestValue(long order, int v1, int v2) {
            this(order, v1, v2, false);
        }

        private TestValue(long order, int v1, int v2, boolean removed) {
            this.order = order;
            this.v1 = v1;
            this.v2 = v2;
            this.removed = removed;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            TestValue testValue = (TestValue) o;

            return order == testValue.order &&
                v1 == testValue.v1 &&
                v2 == testValue.v2;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(order, v1, v2);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestValue{" +
                "order=" + order +
                ", v1=" + v1 +
                ", v2=" + v2 +
                '}';
        }
    }

    /**
     *
     */
    private static class CoordinatorNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            try {
                return node.order() > 1;
            }
            catch (UnsupportedOperationException e) {
                return false;
            }
        }
    }
}
