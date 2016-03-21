package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

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
    public void testVersionIncreaseAtomic() throws Exception {
        checkVersionIncrease(cacheConfiguration(ATOMIC));
    }

    /**
     * @throws Exception If failed.
     */
    public void testVersionIncreaseTx() throws Exception {
        checkVersionIncrease(cacheConfiguration(TRANSACTIONAL));
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void checkVersionIncrease(CacheConfiguration<Object, Object> ccfg) throws Exception {
        try {
            Ignite ignite = startGrid(0);

            IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

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

            Map<Integer, Comparable> vers = new HashMap<>();

            for (Integer k : keys) {
                cache.put(k, k);

                vers.put(k, cache.getEntry(k).version());
            }

            for (int i = 0; i < 10; i++)
                checkVersionIncrease(cache, vers);

            int nodeIdx = 1;

            for (int i = 0; i < 10; i++) {
                startGrid(nodeIdx++);

                checkVersionIncrease(cache, vers);

                awaitPartitionMapExchange();

                checkVersionIncrease(cache, vers);
            }

            for (int i = 1; i < nodeIdx; i++) {
                log.info("Stop node: " + i);

                stopGrid(i);

                checkVersionIncrease(cache, vers);

                awaitPartitionMapExchange();

                checkVersionIncrease(cache, vers);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cache Cache.
     * @param vers Current versions.
     */
    private void checkVersionIncrease(IgniteCache<Object, Object> cache, Map<Integer, Comparable> vers) {
        for (Integer k : vers.keySet()) {
            cache.put(k, k);

            Comparable curVer = vers.get(k);
            Comparable newVer = cache.getEntry(k).version();

            assertTrue(newVer.compareTo(curVer) > 0);

            vers.put(k, newVer);
        }
    }
}
