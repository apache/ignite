package org.apache.ignite.cache.database.db.file;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistenceConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteWalDirectoriesConfigurationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        PersistenceConfiguration pCfg = new PersistenceConfiguration();

        pCfg.setWalStorePath("test/db/wal");

        cfg.setPersistenceConfiguration(pCfg);

        return cfg;
    }

    /**
     *
     */
    public void testPartialWalConfigurationNotAllowed() {
        try {
            startGrid();
        }
        catch (Exception ignore) {
            return;
        }

        fail("Node successfully started with incorrect configuration, exception is expected.");
    }
}
