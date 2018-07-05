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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
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
            .setPageSize(1024)
            .setCheckpointFrequency(10 * 1000)
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
        final long timeOut = U.currentTimeMillis() + 10 * 60 * 1000;

        final int entriesCnt = 10_000;
        final int maxNodesCnt = 4;
        final int topChanges = 50;

        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicBoolean suspend = new AtomicBoolean();

        final ConcurrentMap<Integer, TestValue> map = new ConcurrentHashMap<>();

        Ignite ignite = startGridsMultiThreaded(4);

        ignite.cluster().active(true);

        IgniteCache<Integer, TestValue> cache = ignite.cache(INDEXED_CACHE);

        for (int i = 0; i < entriesCnt; i++) {
            cache.put(i, new TestValue(i, i));
            map.put(i, new TestValue(i, i));
        }

        final AtomicInteger nodesCnt = new AtomicInteger(4);

        IgniteInternalFuture fut = runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (true) {
                    if (stop.get())
                        return null;

                    if (suspend.get()) {
                        U.sleep(10);

                        continue;
                    }

                    int k = ThreadLocalRandom.current().nextInt(entriesCnt);
                    int v1 = ThreadLocalRandom.current().nextInt();
                    int v2 = ThreadLocalRandom.current().nextInt();

                    int n = nodesCnt.get();

                    if (n <= 0)
                        continue;

                    Ignite ignite;

                    try {
                        ignite = grid(ThreadLocalRandom.current().nextInt(n));
                    }
                    catch (Exception ignored) {
                        continue;
                    }

                    if (ignite == null)
                        continue;

                    Transaction tx = null;
                    boolean success = true;

                    if (explicitTx)
                        tx = ignite.transactions().txStart();

                    try {
                        ignite.cache(INDEXED_CACHE).put(k, new TestValue(v1, v2));
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

                    if (success)
                        map.put(k, new TestValue(v1, v2));
                }
            }
        }, 1, "load-runner");

        boolean[] changes = new boolean[] {false, false, true, true};

        try {
            for (int it = 0; it < topChanges; it++) {
                if (U.currentTimeMillis() > timeOut)
                    break;

                U.sleep(3_000);

                boolean add;

                if (it < changes.length)
                    add = changes[it];
                else if (nodesCnt.get() <= maxNodesCnt / 2)
                    add = true;
                else if (nodesCnt.get() >= maxNodesCnt)
                    add = false;
                else // More chance that node will be added
                    add = ThreadLocalRandom.current().nextInt(3) <= 1;

                if (add)
                    startGrid(nodesCnt.getAndIncrement());
                else
                    stopGrid(nodesCnt.decrementAndGet());

                awaitPartitionMapExchange();

                suspend.set(true);

                U.sleep(200);

                for (Map.Entry<Integer, TestValue> entry : map.entrySet())
                    assertEquals(it + " " + Integer.toString(entry.getKey()), entry.getValue(), cache.get(entry.getKey()));

                suspend.set(false);
            }
        }
        finally {
            stop.set(true);
        }

        fut.get();

        awaitPartitionMapExchange();

        for (Map.Entry<Integer, TestValue> entry : map.entrySet())
            assertEquals(Integer.toString(entry.getKey()), entry.getValue(), cache.get(entry.getKey()));
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
        /** V 1. */
        private final int v1;
        /** V 2. */
        private final int v2;

        /**
         * @param v1 V 1.
         * @param v2 V 2.
         */
        private TestValue(int v1, int v2) {
            this.v1 = v1;
            this.v2 = v2;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestValue val = (TestValue)o;

            return v1 == val.v1 && v2 == val.v2;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = v1;

            res = 31 * res + v2;

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestValue{" +
                "v1=" + v1 +
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
