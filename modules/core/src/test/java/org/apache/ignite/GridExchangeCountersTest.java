package org.apache.ignite;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

public class GridExchangeCountersTest extends GridCommonAbstractTest {

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        memCfg.setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);
        memCfg.setPageSize(1024);
        memCfg.setWalMode(WALMode.LOG_ONLY);

        DataRegionConfiguration memPlcCfg = new DataRegionConfiguration();

        memPlcCfg.setName("dfltDataRegion");
        memPlcCfg.setMaxSize(150 * 1024 * 1024);
        memPlcCfg.setInitialSize(100 * 1024 * 1024);
        memPlcCfg.setPersistenceEnabled(true);

        memCfg.setDefaultDataRegionConfiguration(memPlcCfg);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg = new CacheConfiguration()
                .setName("cache")
                .setCacheMode(CacheMode.REPLICATED)
                .setRebalanceMode(CacheRebalanceMode.SYNC)
                .setAffinity(new RendezvousAffinityFunction(false, 5));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    @Override
    protected void beforeTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, true));
    }

    @Override
    protected void afterTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, true));
    }

    public void testCounters() throws Exception {
        IgniteEx grid = (IgniteEx) startGrids(3);
        grid.active(true);

        for (int i = 0; i < 10; i++) {
            grid.cache("cache").put(i, i);
        }

        stopGrid(2);

        for (int i = 10; i < 20; i++) {
            grid.cache("cache").put(i, i);
        }

        stopGrid(1);

        for (int i = 20; i < 30; i++) {
            grid.cache("cache").put(i, i);
        }

        startGrid(1);

        for (int i = 30; i < 40; i++) {
            grid.cache("cache").put(i, i);
        }

        startGrid(2);

        for (int i = 40; i < 50; i++) {
            grid.cache("cache").put(i, i);
        }

        stopGrid(0);

        for (int i = 50; i < 60; i++) {
            grid.cache("cache").put(i, i);
        }

        startGrid(0);
        Thread.sleep(10000);
    }


}
