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

package org.apache.ignite.ml.dlearn.context.cache;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformers;
import org.apache.ignite.ml.dlearn.dataset.DLearnDataset;
import org.apache.ignite.ml.dlearn.utils.DLearnContextPartitionKey;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link CacheDLearnContext}.
 */
public class CacheDLearnContextTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 4;

    /** */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /** */
    public void testClose() {
        IgniteCache<Integer, String> data = generateTestData(2, 0);

        CacheDLearnContextFactory<Integer, String> ctxFactory = new CacheDLearnContextFactory<>(ignite, data);
        CacheDLearnContext<CacheDLearnPartition<Integer, String>> ctx = ctxFactory.createContext();
        IgniteCache<DLearnContextPartitionKey, byte[]> learningCtxCache = ignite.cache(ctx.getLearningCtxCacheName());

        // context cache contains 2 partitions, each partition contains cache name and partition number
        assertEquals(4, learningCtxCache.size());

        ctx.close();

        // all data were removed from context cache
        assertEquals(0, learningCtxCache.size());
    }

    /** */
    public void testCloseDerivativeContext() {
        IgniteCache<Integer, String> data = generateTestData(2, 0);

        CacheDLearnContextFactory<Integer, String> ctxFactory = new CacheDLearnContextFactory<>(ignite, data);
        CacheDLearnContext<CacheDLearnPartition<Integer, String>> ctx = ctxFactory.createContext();
        IgniteCache<DLearnContextPartitionKey, byte[]> learningCtxCache = ignite.cache(ctx.getLearningCtxCacheName());

        // context cache contains 2 partitions, each partition contains cache name and partition number
        assertEquals(4, learningCtxCache.size());

        DLearnDataset<?> dataset = ctx.transform(DLearnContextTransformers.cacheToDataset((k, v) -> new double[0]));

        // features and rows were added into both partitions
        assertEquals(8, learningCtxCache.size());

        dataset.close();

        // features and rows were removed
        assertEquals(4, learningCtxCache.size());

        ctx.close();

        // all data were removed from context cache
        assertEquals(0, learningCtxCache.size());
    }

    /** */
    public void testCloseBaseContext() {
        IgniteCache<Integer, String> data = generateTestData(2, 0);

        CacheDLearnContextFactory<Integer, String> ctxFactory = new CacheDLearnContextFactory<>(ignite, data);
        CacheDLearnContext<CacheDLearnPartition<Integer, String>> ctx = ctxFactory.createContext();
        IgniteCache<DLearnContextPartitionKey, byte[]> learningCtxCache = ignite.cache(ctx.getLearningCtxCacheName());

        // context cache contains 2 partitions, each partition contains cache name and partition number
        assertEquals(4, learningCtxCache.size());

        DLearnDataset<?> dataset = ctx.transform(DLearnContextTransformers.cacheToDataset((k, v) -> new double[0]));

        // features and rows were added into both partitions
        assertEquals(8, learningCtxCache.size());

        ctx.close();

        // 2 partitions with initial data were removed
        assertEquals(4, learningCtxCache.size());

        dataset.close();

        // all data were removed from context cache
        assertEquals(0, learningCtxCache.size());
    }

    /**
     * Generates Ignite Cache with data for tests.
     *
     * @return Ignite Cache with data for tests
     */
    private IgniteCache<Integer, String> generateTestData(int partitions, int backups) {
        CacheConfiguration<Integer, String> cacheConfiguration = new CacheConfiguration<>();

        cacheConfiguration.setName(UUID.randomUUID().toString());
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, partitions));
        cacheConfiguration.setBackups(backups);

        IgniteCache<Integer, String> cache = ignite.createCache(cacheConfiguration);

        cache.put(1, "TEST1");
        cache.put(2, "TEST2");
        cache.put(3, "TEST3");
        cache.put(4, "TEST4");

        return cache;
    }
}
