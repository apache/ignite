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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 * Test for rebalancing and persistence integration.
 */
public abstract class IgnitePdsCacheRebalancingAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String cacheName = "cache";

    /** */
    protected boolean explicitTx;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg1 = cacheConfiguration(cacheName);
        ccfg1.setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);
        ccfg1.setBackups(1);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        CacheConfiguration ccfg2 = cacheConfiguration("indexed");
        ccfg2.setBackups(1);
        ccfg2.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        QueryEntity qryEntity = new QueryEntity(Integer.class.getName(), TestValue.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("v1", Integer.class.getName());
        fields.put("v2", Integer.class.getName());

        qryEntity.setFields(fields);

        QueryIndex qryIdx = new QueryIndex("v1", true);

        qryEntity.setIndexes(Collections.singleton(qryIdx));

        ccfg2.setQueryEntities(Collections.singleton(qryEntity));

        // Do not start filtered cache on coordinator.
        if (gridName.endsWith("0")) {
            cfg.setCacheConfiguration(ccfg1, ccfg2);
        }
        else {
            CacheConfiguration ccfg3 = cacheConfiguration("filtered");
            ccfg3.setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);
            ccfg3.setBackups(1);
            ccfg3.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            ccfg3.setNodeFilter(new CoordinatorNodeFilter());

            cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3);
        }

        MemoryConfiguration memCfg = new MemoryConfiguration();

        memCfg.setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);
        memCfg.setPageSize(1024);

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setMaxSize(150 * 1024 * 1024);
        memPlcCfg.setInitialSize(100 * 1024 * 1024);
        memPlcCfg.setSwapFilePath("work/swap");

        memCfg.setMemoryPolicies(memPlcCfg);
        memCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(memCfg);

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
        );

        cfg.setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(IP_FINDER)
        );

        return cfg;
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

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /**
     * Test that outdated partitions on restarted nodes are correctly replaced with newer versions.
     *
     * @throws Exception If fails.
     */
    public void testRebalancingOnRestart() throws Exception {
        Ignite ignite0 = startGrid(0);

        ignite0.active(true);

        startGrid(1);

        IgniteEx ignite2 = startGrid(2);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1 = ignite0.cache(cacheName);

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

        IgniteCache<Integer, Integer> cache3 = ignite2.cache(cacheName);

        for (int i = 0; i < 100; i++)
            assertEquals(String.valueOf(i), (Integer)(i * 2), cache3.get(i));
    }

    /**
     * Test that outdated partitions on restarted nodes are correctly replaced with newer versions.
     *
     * @throws Exception If fails.
     */
    public void testRebalancingOnRestartAfterCheckpoint() throws Exception {
        fail("IGNITE-5302");

        IgniteEx ignite0 = startGrid(0);

        IgniteEx ignite1 = startGrid(1);

        IgniteEx ignite2 = startGrid(2);
        IgniteEx ignite3 = startGrid(3);

        ignite0.active(true);

        ignite0.cache(cacheName).rebalance().get();
        ignite1.cache(cacheName).rebalance().get();
        ignite2.cache(cacheName).rebalance().get();
        ignite3.cache(cacheName).rebalance().get();

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1 = ignite0.cache(cacheName);

        for (int i = 0; i < 1000; i++)
            cache1.put(i, i);

        ignite0.context().cache().context().database().waitForCheckpoint("test");
        ignite1.context().cache().context().database().waitForCheckpoint("test");

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

        ignite2 = startGrid(2);
        ignite3 = startGrid(3);

        ignite2.cache(cacheName).rebalance().get();
        ignite3.cache(cacheName).rebalance().get();

        IgniteCache<Integer, Integer> cache2 = ignite2.cache(cacheName);
        IgniteCache<Integer, Integer> cache3 = ignite3.cache(cacheName);

        for (int i = 0; i < 100; i++) {
            assertEquals(String.valueOf(i), (Integer)(i * 2), cache2.get(i));
            assertEquals(String.valueOf(i), (Integer)(i * 2), cache3.get(i));
        }
    }

    /**
     * Test that all data is correctly restored after non-graceful restart.
     *
     * @throws Exception If fails.
     */
    public void testDataCorrectnessAfterRestart() throws Exception {
        IgniteEx ignite1 = (IgniteEx)G.start(getConfiguration("test1"));
        IgniteEx ignite2 = (IgniteEx)G.start(getConfiguration("test2"));
        IgniteEx ignite3 = (IgniteEx)G.start(getConfiguration("test3"));
        IgniteEx ignite4 = (IgniteEx)G.start(getConfiguration("test4"));

        ignite1.active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1 = ignite1.cache(cacheName);

        for (int i = 0; i < 100; i++)
            cache1.put(i, i);

        ignite1.close();
        ignite2.close();
        ignite3.close();
        ignite4.close();

        ignite1 = (IgniteEx)G.start(getConfiguration("test1"));
        ignite2 = (IgniteEx)G.start(getConfiguration("test2"));
        ignite3 = (IgniteEx)G.start(getConfiguration("test3"));
        ignite4 = (IgniteEx)G.start(getConfiguration("test4"));

        ignite1.active(true);

        awaitPartitionMapExchange();

        cache1 = ignite1.cache(cacheName);
        IgniteCache<Integer, Integer> cache2 = ignite2.cache(cacheName);
        IgniteCache<Integer, Integer> cache3 = ignite3.cache(cacheName);
        IgniteCache<Integer, Integer> cache4 = ignite4.cache(cacheName);

        for (int i = 0; i < 100; i++) {
            assert cache1.get(i).equals(i);
            assert cache2.get(i).equals(i);
            assert cache3.get(i).equals(i);
            assert cache4.get(i).equals(i);
        }
    }

    /**
     * Test that partitions are marked as lost when all owners leave cluster, but recover after nodes rejoin.
     *
     * @throws Exception If fails.
     */
    public void testPartitionLossAndRecover() throws Exception {
        fail("IGNITE-5302");

        Ignite ignite1 = startGrid(0);
        Ignite ignite2 = startGrid(1);
        Ignite ignite3 = startGrid(2);
        Ignite ignite4 = startGrid(3);

        awaitPartitionMapExchange();

        IgniteCache<String, String> cache1 = ignite1.cache(cacheName);

        final int offset = 10;

        for (int i = 0; i < 100; i++)
            cache1.put(String.valueOf(i), String.valueOf(i + offset));

        ignite3.close();
        ignite4.close();

        awaitPartitionMapExchange();

        assert !ignite1.cache(cacheName).lostPartitions().isEmpty();

        ignite3 = startGrid(2);
        ignite4 = startGrid(3);

        ignite1.resetLostPartitions(Collections.singletonList(cacheName));

        IgniteCache<String, String> cache2 = ignite2.cache(cacheName);
        IgniteCache<String, String> cache3 = ignite3.cache(cacheName);
        IgniteCache<String, String> cache4 = ignite4.cache(cacheName);

        //Thread.sleep(5_000);

        for (int i = 0; i < 100; i++) {
            String key = String.valueOf(i);
            String expected = String.valueOf(i + offset);

            assertEquals(expected, cache1.get(key));
            assertEquals(expected, cache2.get(key));
            assertEquals(expected, cache3.get(key));
            assertEquals(expected, cache4.get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopologyChangesWithConstantLoad() throws Exception {
        final long timeOut = U.currentTimeMillis() + 10 * 60 * 1000;

        final int entriesCnt = 10_000;
        int maxNodesCount = 4;
        int topChanges = 20;
        final String cacheName = "indexed";

        final AtomicBoolean stop = new AtomicBoolean();

        final ConcurrentMap<Integer, TestValue> map = new ConcurrentHashMap<>();

        Ignite ignite = startGrid(0);

        ignite.active(true);

        IgniteCache<Integer, TestValue> cache = ignite.cache(cacheName);

        for (int i = 0; i < entriesCnt; i++) {
            cache.put(i, new TestValue(i, i));
            map.put(i, new TestValue(i, i));
        }

        final AtomicInteger nodesCnt = new AtomicInteger();

        IgniteInternalFuture fut = runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (true) {
                    if (stop.get())
                        return null;

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
                        ignite.cache(cacheName).put(k, new TestValue(v1, v2));
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

        try {
            for (int i = 0; i < topChanges; i++) {
                if (U.currentTimeMillis() > timeOut)
                    break;

                U.sleep(3_000);

                boolean add;

                if (nodesCnt.get() <= maxNodesCount / 2)
                    add = true;
                else if (nodesCnt.get() > maxNodesCount)
                    add = false;
                else // More chance that node will be added
                    add = ThreadLocalRandom.current().nextInt(3) <= 1;

                if (add)
                    startGrid(nodesCnt.incrementAndGet());
                else
                    stopGrid(nodesCnt.getAndDecrement());

                awaitPartitionMapExchange();

                cache.rebalance().get();
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
        testForceRebalance(cacheName);
    }

    /**
     * @throws Exception If failed.
     */
    public void testForceRebalanceClientTopology() throws Exception {
        testForceRebalance("filtered");
    }

    /**
     * @throws Exception If failed.
     */
    private void testForceRebalance(String cacheName) throws Exception {
        startGrids(4);

        final Ignite ig = grid(1);

        ig.active(true);

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

        ig.active(true);

        int k = 0;

        try (IgniteDataStreamer ds = ig.dataStreamer(cacheName)) {
            ds.allowOverwrite(true);

            for (int k0 = k; k < k0 + 10_000; k++)
                ds.addData(k, k);
        }

        for (int t = 0; t < 10; t++) {
            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        stopGrid(3);

                        IgniteEx ig0 = startGrid(3);

                        awaitPartitionMapExchange();

                        ig0.cache(cacheName).rebalance().get();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            try (IgniteDataStreamer ds = ig.dataStreamer(cacheName)) {
                ds.allowOverwrite(true);

                while (!fut.isDone()) {
                    ds.addData(k, k);

                    k++;

                    U.sleep(1);
                }
            }

            fut.get();

            Map<Integer, Long> cntrs = new HashMap<>();

            for (int g = 0; g < 4; g++) {
                IgniteEx ig0 = grid(g);

                for (GridDhtLocalPartition part : ig0.cachex(cacheName).context().topology().currentLocalPartitions()) {
                    if (cntrs.containsKey(part.id()))
                        assertEquals(String.valueOf(part.id()), (long) cntrs.get(part.id()), part.updateCounter());
                    else
                        cntrs.put(part.id(), part.updateCounter());
                }

                for (int k0 = 0; k0 < k; k0++)
                    assertEquals(String.valueOf(k0), k0, ig0.cache(cacheName).get(k0));
            }

            assertEquals(ig.affinity(cacheName).partitions(), cntrs.size());
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
            return node.order() > 1;
        }
    }
}
