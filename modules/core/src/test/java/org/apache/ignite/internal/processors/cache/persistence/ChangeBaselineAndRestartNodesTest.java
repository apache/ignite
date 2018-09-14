package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 */
public class ChangeBaselineAndRestartNodesTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name)
            .setConsistentId(name)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER))
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(2))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setCheckpointFrequency(2L * 60 * 1000)
                    .setWalMode(WALMode.LOG_ONLY)
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setMaxSize(200L * 1024 * 1024)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    private static MetaStorage getMataStorage(Ignite ignite) {
        return ((IgniteEx)ignite).context().cache().context().database().metaStorage();
    }

    /**
     *
     */
    public void test() throws Exception {
        final IgniteEx ignite0 = (IgniteEx)startGrids(3);
        ignite0.cluster().active(true);

        IgniteEx ignite2 = grid(2);

        IgniteCache cache = ignite0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 1000; j++)
                cache.put(i + j, "val");

            ((IgniteProcessProxy)ignite2).kill();

            ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

            ignite2 = startGrid(2);

            ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());
        }
    }
}
