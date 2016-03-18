package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class GridCacheVersionTopologyChangeTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testVersionIncrease() throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration();

        Ignite ignite = startGrid(0);

        IgniteCache cache = ignite.createCache(ccfg);

        Affinity<Object> aff = ignite.affinity(ccfg.getName());

        int parts = aff.partitions();

        assert parts > 0 : parts;

        Set<Integer> keys = new HashSet<>();

        for (int p = 0; p < parts; p++) {
            for (int k = 0; k < 100_000; k++) {
                if (aff.partition(k) == p) {
                    assertTrue(keys.add(k));

                    break;
                }
            }
        }

        assertEquals(parts, keys.size());
    }
}
