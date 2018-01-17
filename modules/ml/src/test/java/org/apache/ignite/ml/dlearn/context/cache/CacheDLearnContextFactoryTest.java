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
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link CacheDLearnContextFactory}.
 */
public class CacheDLearnContextFactoryTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 10;

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

    /**
     * Test that partitions of the context created on top of the dataset are placed on the same nodes as initial
     * dataset (number of partitions are less than number of nodes).
     */
    public void testAffinityWithNumberOfPartitionsLessThanNodes() {
        testAffinity(generateTestData(5, 10), 5);
    }

    /**
     * Test that partitions of the context created on top of the dataset are placed on the same nodes as initial
     * dataset (number of partitions are greater than number of nodes).
     */
    public void testAffinityWithNumberOfPartitionsGreaterThanNodes() {
        testAffinity(generateTestData(50, 10), 50);
    }

    /** */
    private void testAffinity(IgniteCache<Integer, String> data, int partitions) {
        CacheDLearnContextFactory<Integer, String> ctxFactory = new CacheDLearnContextFactory<>(ignite, data);

        CacheDLearnContext<CacheDLearnPartition<Integer, String>> ctx = ctxFactory.createContext();

        Affinity<?> dataAffinity = ignite.affinity(data.getName());
        Affinity<?> ctxAffinity = ignite.affinity(ctx.getLearningCtxCacheName());

        assertEquals(partitions, dataAffinity.partitions());
        assertEquals(partitions, ctxAffinity.partitions());

        for (int part = 0; part < partitions; part++) {
            ClusterNode dataPartPrimaryNode = dataAffinity.mapPartitionToNode(part);
            ClusterNode ctxPartPrimaryNode = ctxAffinity.mapPartitionToNode(part);

            assertNotNull(dataPartPrimaryNode);
            assertNotNull(ctxPartPrimaryNode);
            assertEquals(dataPartPrimaryNode, ctxPartPrimaryNode);
        }
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

        for (int i = 0; i < partitions * 10; i++)
            cache.put(i, String.valueOf(i));

        return cache;
    }
}