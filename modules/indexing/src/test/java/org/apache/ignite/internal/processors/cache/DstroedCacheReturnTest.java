package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 */
public class DstroedCacheReturnTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int CACHES = 3;
    /** */
    private static final int NODES = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(ipFinder))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setCheckpointFrequency(1_000)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200 * 1024 * 1024)
                    .setPersistenceEnabled(true)));
    }

    /**
     *
     */
    private void clearDiskStorage() throws IgniteCheckedException {
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", true);
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", true);
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", true);
    }

    public void test() throws Exception {
        IgniteEx ignite0 = (IgniteEx)startGrids(NODES);

        ignite0.cluster().active(true);

        statCachesDynamically(ignite0);

        loadCaches(ignite0);

        ignite0.cache("cache0").destroy();
        ignite0.cache("cache1").destroy();

        assertEquals(ignite0.cacheNames().size(), CACHES - 2);

        ignite0.cluster().active(false);

        Thread.sleep(10_000);

        info("WHOLE GRID DE-ACTIVE");

        ignite0.cluster().active(true);

//        stopAllGrids();

//        info("WHOLE GRID STOPPED");

//        ignite0 = (IgniteEx)startGrids(NODES);

        for (String cacheName: ignite0.cacheNames())
            info("!!! " + cacheName);

//        assertEquals(ignite0.cacheNames().size(), CACHES - 2);
    }


    /**
     * @param ignite Ignite.
     */
    private void statCachesDynamically(IgniteEx ignite) {
        List<CacheConfiguration> ccfg = new ArrayList<>(CACHES);

        for (int i = 0; i < CACHES; i++)
            ccfg.add(new CacheConfiguration<>("cache" + i)
                .setGroupName(i % 2 == 0 ? "grp-even" : "grp-odd")
                .setBackups(1)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setQueryEntities(Collections.singletonList(
                    new QueryEntity(Integer.class.getName(), CacheValueObj2.class.getName())
                        .addQueryField("name", String.class.getName(), null)
                        .addQueryField("id", Long.class.getName(), null)
                        .setIndexes(Collections.singletonList(new QueryIndex()
                            .setFieldNames(Collections.singletonList("name"), true))))));

        ignite.createCaches(ccfg);
    }

    /**
     *
     */
    private static class CacheValueObj2 {
        /** Payload. */
        private byte[] payload = new byte[10 * 1024];
        /** Marker. */
        private String name;

        private long id;

        /**
         * @param name Marker.
         */
        public CacheValueObj2(String name) {
            this.name = name;
            this.id = name.hashCode();
        }
    }

    /**
     * @param ignite Ign 1.
     */
    private void loadCaches(IgniteEx ignite) {
        for (int i = 0; i < CACHES; i++) {
            IgniteCache cache = ignite.cache("cache" + i);

//            if (i % 2 == 1)
//                continue;

            for (int j = 0; j < 100; j++)
                cache.put(j, new CacheValueObj2((i % 2 == 0 ? "1 str " : "2 str ") + j));
        }
    }
}
