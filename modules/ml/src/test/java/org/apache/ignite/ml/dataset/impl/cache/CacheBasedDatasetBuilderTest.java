/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.dataset.impl.cache;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CacheBasedDatasetBuilder}.
 */
@RunWith(JUnit4.class)
public class CacheBasedDatasetBuilderTest extends GridCommonAbstractTest {
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
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /**
     * Tests that partitions of the dataset cache are placed on the same nodes as upstream cache.
     */
    @Test
    public void testBuild() {
        IgniteCache<Integer, String> upstreamCache = createTestCache(100, 10);
        CacheBasedDatasetBuilder<Integer, String> builder = new CacheBasedDatasetBuilder<>(ignite, upstreamCache);

        CacheBasedDataset<Integer, String, Long, AutoCloseable> dataset = builder.build(
            (upstream, upstreamSize) -> upstreamSize,
            (upstream, upstreamSize, ctx) -> null
        );

        Affinity<Integer> upstreamAffinity = ignite.affinity(upstreamCache.getName());
        Affinity<Integer> datasetAffinity = ignite.affinity(dataset.getDatasetCache().getName());

        int upstreamPartitions = upstreamAffinity.partitions();
        int datasetPartitions = datasetAffinity.partitions();

        assertEquals(upstreamPartitions, datasetPartitions);

        for (int part = 0; part < upstreamPartitions; part++) {
            Collection<ClusterNode> upstreamPartNodes = upstreamAffinity.mapPartitionToPrimaryAndBackups(part);
            Collection<ClusterNode> datasetPartNodes = datasetAffinity.mapPartitionToPrimaryAndBackups(part);

            assertEqualsCollections(upstreamPartNodes, datasetPartNodes);
        }
    }

    /**
     * Tests that predicate works correctly.
     */
    @Test
    public void testBuildWithPredicate() {
        CacheConfiguration<Integer, Integer> upstreamCacheConfiguration = new CacheConfiguration<>();
        upstreamCacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 1));
        upstreamCacheConfiguration.setName(UUID.randomUUID().toString());

        IgniteCache<Integer, Integer> upstreamCache = ignite.createCache(upstreamCacheConfiguration);
        upstreamCache.put(1, 1);
        upstreamCache.put(2, 2);

        CacheBasedDatasetBuilder<Integer, Integer> builder = new CacheBasedDatasetBuilder<>(
            ignite,
            upstreamCache,
            (k, v) -> k % 2 == 0
        );

        CacheBasedDataset<Integer, Integer, Long, AutoCloseable> dataset = builder.build(
            (upstream, upstreamSize) -> {
                UpstreamEntry<Integer, Integer> entry = upstream.next();
                assertEquals(Integer.valueOf(2), entry.getKey());
                assertEquals(Integer.valueOf(2), entry.getValue());
                assertFalse(upstream.hasNext());
                return 0L;
            },
            (upstream, upstreamSize, ctx) -> {
                UpstreamEntry<Integer, Integer> entry = upstream.next();
                assertEquals(Integer.valueOf(2), entry.getKey());
                assertEquals(Integer.valueOf(2), entry.getValue());
                assertFalse(upstream.hasNext());
                return null;
            }
        );

        dataset.compute(data -> {});
    }

    /**
     * Generate an Ignite Cache with the specified size and number of partitions for testing purposes.
     *
     * @param size Size of an Ignite Cache.
     * @param parts Number of partitions.
     * @return Ignite Cache instance.
     */
    private IgniteCache<Integer, String> createTestCache(int size, int parts) {
        CacheConfiguration<Integer, String> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName(UUID.randomUUID().toString());
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, parts));

        IgniteCache<Integer, String> cache = ignite.createCache(cacheConfiguration);

        for (int i = 0; i < size; i++)
            cache.put(i, "DATA_" + i);

        return cache;
    }
}
