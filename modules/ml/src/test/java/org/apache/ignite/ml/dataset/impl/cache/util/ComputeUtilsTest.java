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

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.UpstreamTransformerBuilder;
import org.apache.ignite.ml.environment.deploy.DeployingContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link ComputeUtils}.
 */
public class ComputeUtilsTest extends GridCommonAbstractTest {
    /** Number of nodes in grid. */
    private static final int NODE_COUNT = 10;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /**
     * Tests that in case two caches maintain their partitions on different nodes, affinity call won't be completed.
     */
    @Test
    public void testAffinityCallWithRetriesNegative() {
        ClusterNode node1 = grid(1).cluster().localNode();
        ClusterNode node2 = grid(2).cluster().localNode();

        String firstCacheName = "CACHE_1_" + UUID.randomUUID();
        String secondCacheName = "CACHE_2_" + UUID.randomUUID();

        CacheConfiguration<Integer, Integer> cacheConfiguration1 = new CacheConfiguration<>();
        cacheConfiguration1.setName(firstCacheName);
        cacheConfiguration1.setAffinity(new TestAffinityFunction(node1));
        IgniteCache<Integer, Integer> cache1 = ignite.createCache(cacheConfiguration1);

        CacheConfiguration<Integer, Integer> cacheConfiguration2 = new CacheConfiguration<>();
        cacheConfiguration2.setName(secondCacheName);
        cacheConfiguration2.setAffinity(new TestAffinityFunction(node2));
        IgniteCache<Integer, Integer> cache2 = ignite.createCache(cacheConfiguration2);

        try {
            try {
                ComputeUtils.affinityCallWithRetries(
                    ignite,
                    Arrays.asList(firstCacheName, secondCacheName),
                    part -> part,
                    0,
                    DeployingContext.unitialized()
                );
            }
            catch (IllegalStateException expectedException) {
                return;
            }

            fail("Missing IllegalStateException");
        }
        finally {
            cache1.destroy();
            cache2.destroy();
        }
    }

    /**
     * Test that in case two caches maintain their partitions on the same node, affinity call will be completed.
     */
    @Test
    public void testAffinityCallWithRetriesPositive() {
        ClusterNode node = grid(1).cluster().localNode();

        String firstCacheName = "CACHE_1_" + UUID.randomUUID();
        String secondCacheName = "CACHE_2_" + UUID.randomUUID();

        CacheConfiguration<Integer, Integer> cacheConfiguration1 = new CacheConfiguration<>();
        cacheConfiguration1.setName(firstCacheName);
        cacheConfiguration1.setAffinity(new TestAffinityFunction(node));
        IgniteCache<Integer, Integer> cache1 = ignite.createCache(cacheConfiguration1);

        CacheConfiguration<Integer, Integer> cacheConfiguration2 = new CacheConfiguration<>();
        cacheConfiguration2.setName(secondCacheName);
        cacheConfiguration2.setAffinity(new TestAffinityFunction(node));
        IgniteCache<Integer, Integer> cache2 = ignite.createCache(cacheConfiguration2);

        try (IgniteAtomicLong cnt = ignite.atomicLong("COUNTER_" + UUID.randomUUID(), 0, true)) {

            ComputeUtils.affinityCallWithRetries(ignite, Arrays.asList(firstCacheName, secondCacheName), part -> {
                Ignite locIgnite = Ignition.localIgnite();

                assertEquals(node, locIgnite.cluster().localNode());

                cnt.incrementAndGet();

                return part;
            }, 0, DeployingContext.unitialized());

            assertEquals(1, cnt.get());
        }
        finally {
            cache1.destroy();
            cache2.destroy();
        }
    }

    /**
     * Tests {@code getData()} method.
     */
    @Test
    public void testGetData() {
        ClusterNode node = grid(1).cluster().localNode();

        String upstreamCacheName = "CACHE_1_" + UUID.randomUUID();
        String datasetCacheName = "CACHE_2_" + UUID.randomUUID();

        CacheConfiguration<Integer, Integer> upstreamCacheConfiguration = new CacheConfiguration<>();
        upstreamCacheConfiguration.setName(upstreamCacheName);
        upstreamCacheConfiguration.setAffinity(new TestAffinityFunction(node));
        IgniteCache<Integer, Integer> upstreamCache = ignite.createCache(upstreamCacheConfiguration);

        CacheConfiguration<Integer, Integer> datasetCacheConfiguration = new CacheConfiguration<>();
        datasetCacheConfiguration.setName(datasetCacheName);
        datasetCacheConfiguration.setAffinity(new TestAffinityFunction(node));
        IgniteCache<Integer, Integer> datasetCache = ignite.createCache(datasetCacheConfiguration);

        upstreamCache.put(42, 42);
        datasetCache.put(0, 0);

        UUID datasetId = UUID.randomUUID();

        IgniteAtomicLong cnt = ignite.atomicLong("CNT_" + datasetId, 0, true);

        for (int i = 0; i < 10; i++) {
            Collection<TestPartitionData> data = ComputeUtils.affinityCallWithRetries(
                ignite,
                Arrays.asList(datasetCacheName, upstreamCacheName),
                part -> ComputeUtils.<Integer, Integer, Serializable, TestPartitionData>getData(
                    ignite,
                    upstreamCacheName,
                    (k, v) -> true,
                    UpstreamTransformerBuilder.identity(),
                    datasetCacheName,
                    datasetId,
                    (env, upstream, upstreamSize, ctx) -> {
                        cnt.incrementAndGet();

                        assertEquals(1, upstreamSize);

                        UpstreamEntry<Integer, Integer> e = upstream.next();
                        return new TestPartitionData(e.getKey() + e.getValue());
                    },
                    TestUtils.testEnvBuilder().buildForWorker(part),
                    false
                ),
                0,
                DeployingContext.unitialized()
            );

            assertEquals(1, data.size());

            TestPartitionData dataElement = data.iterator().next();
            assertEquals(84, dataElement.val.intValue());
        }

        assertEquals(1, cnt.get());
    }

    /**
     * Tests {@code initContext()} method.
     */
    @Test
    public void testInitContext() {
        ClusterNode node = grid(1).cluster().localNode();

        String upstreamCacheName = "CACHE_1_" + UUID.randomUUID();
        String datasetCacheName = "CACHE_2_" + UUID.randomUUID();

        CacheConfiguration<Integer, Integer> upstreamCacheConfiguration = new CacheConfiguration<>();
        upstreamCacheConfiguration.setName(upstreamCacheName);
        upstreamCacheConfiguration.setAffinity(new TestAffinityFunction(node));
        IgniteCache<Integer, Integer> upstreamCache = ignite.createCache(upstreamCacheConfiguration);

        CacheConfiguration<Integer, Integer> datasetCacheConfiguration = new CacheConfiguration<>();
        datasetCacheConfiguration.setName(datasetCacheName);
        datasetCacheConfiguration.setAffinity(new TestAffinityFunction(node));
        IgniteCache<Integer, Integer> datasetCache = ignite.createCache(datasetCacheConfiguration);

        upstreamCache.put(42, 42);

        ComputeUtils.<Integer, Integer, Integer>initContext(
            ignite,
            upstreamCacheName,
            UpstreamTransformerBuilder.identity(),
            (k, v) -> true,
            datasetCacheName,
            (env, upstream, upstreamSize) -> {

                assertEquals(1, upstreamSize);

                UpstreamEntry<Integer, Integer> e = upstream.next();
                return e.getKey() + e.getValue();
            },
            TestUtils.testEnvBuilder(),
            0,
            0,
            false,
            DeployingContext.unitialized()
        );

        assertEquals(1, datasetCache.size());
        assertEquals(84, datasetCache.get(0).intValue());
    }

    /**
     * Test partition data.
     */
    private static class TestPartitionData implements AutoCloseable {
        /** Value. */
        private final Integer val;

        /**
         * Constructs a new instance of test partition data.
         *
         * @param val Value.
         */
        TestPartitionData(Integer val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            // Do nothing, GC will clean up.
        }
    }

    /**
     * Affinity function used in tests in this class. Defines one partition and assign it on the specified cluster node.
     */
    private static class TestAffinityFunction implements AffinityFunction {
        /** */
        private static final long serialVersionUID = -1353725303983563094L;

        /** Cluster node partition will be assigned on. */
        private final ClusterNode node;

        /**
         * Constructs a new instance of test affinity function.
         *
         * @param node Cluster node partition will be assigned on.
         */
        TestAffinityFunction(ClusterNode node) {
            this.node = node;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // Do nothing.
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            return Collections.singletonList(Collections.singletonList(node));
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // Do nothing.
        }
    }
}
