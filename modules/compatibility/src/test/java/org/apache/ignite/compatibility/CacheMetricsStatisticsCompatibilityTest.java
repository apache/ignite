package org.apache.ignite.compatibility;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/**
 *
 */
public class CacheMetricsStatisticsCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** Cache name 1. */
    private static String CACHE_NAME_1 = "cache1";

    /** Cache name 2. */
    private static String CACHE_NAME_2 = "cache2";

    /** Topology validator */
    private static class NodeCountValidator implements  TopologyValidator, Serializable {
        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            return nodes.size() == 2;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cCfg1 = new CacheConfiguration()
            .setName(CACHE_NAME_1)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_ALL);

        CacheConfiguration cCfg2 = new CacheConfiguration()
            .setName(CACHE_NAME_2)
            .setCacheMode(CacheMode.REPLICATED)
            .setTopologyValidator(new NodeCountValidator());

        cfg.setCacheConfiguration(cCfg1, cCfg2);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Asserts that the cache has appropriate status (indicated by the cache metrics).
     *
     * @param cacheName Cache name.
     * @param validForReading Cache is valid for reading.
     * @param validForWriting Cache is valid for writing.
     */
    void assertCacheStatus(String cacheName, boolean validForReading, boolean validForWriting) {
        List<Ignite> nodes = G.allGrids();

        assertFalse(nodes.isEmpty());

        for (Ignite node : nodes) {
            assertEquals(validForReading, node.cache(cacheName).metrics().isValidForReading());
            assertEquals(validForWriting, node.cache(cacheName).metrics().isValidForWriting());
        }
    }

    /**
     * Test the cache validator metrics.
     * Cache can be invalid for writing due to invalid topology or due to partitions loss.
     * Now we can't reproduce test case with invalid for reading cache. At present, reading from the cache can be
     * invalid only for certain keys or when the cluster is not active (in this case caches are not available at all).
     *
     * @throws Exception If failed.
     */
    public void testCacheValidatorMetrics() throws Exception {
        startGrid(1, "2.3.0", new ConfigurationClosure());
        startGrid(0);

        awaitPartitionMapExchange();

        assertCacheStatus(CACHE_NAME_1, true, true);
        assertCacheStatus(CACHE_NAME_2, true, true);

        stopGrid(1);

        awaitPartitionMapExchange();

        assertCacheStatus(CACHE_NAME_1, true, false);
        assertCacheStatus(CACHE_NAME_2, true, false);
    }

    /** */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);
        }
    }
}
