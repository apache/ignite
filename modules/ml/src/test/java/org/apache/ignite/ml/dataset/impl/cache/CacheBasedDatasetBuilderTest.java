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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link CacheBasedDatasetBuilder}.
 */
public class CacheBasedDatasetBuilderTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
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
     * Tests that partitions of the dataset cache are placed on the same nodes as upstream cache.
     */
    public void testBuild() {
        IgniteCache<Integer, String> upstreamCache = createTestCache(100, 10);
        CacheBasedDatasetBuilder<Integer, String, Long, AutoCloseable> builder =
            new CacheBasedDatasetBuilder<>(
                ignite,
                upstreamCache,
                (upstream, upstreamSize) -> upstreamSize,
                (upstream, upstreamSize, ctx) -> null
            );

        CacheBasedDataset<Integer, String, Long, AutoCloseable> dataset = builder.build();

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
     * Generate an Ignite Cache with the specified size and number of partitions for testing purposes.
     *
     * @param size size of an Ignite Cache
     * @param parts number of partitions
     * @return Ignite Cache instance
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
