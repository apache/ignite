package org.apache.ignite.cache.database;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistenceConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.database.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgnitePersistentStorePageSizesSelfTest extends GridCommonAbstractTest {
    /** */
    private int pageSize;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        MemoryConfiguration memCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setSize(100 * 1024 * 1024);

        memCfg.setMemoryPolicies(memPlcCfg);
        memCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        memCfg.setPageSize(pageSize);

        cfg.setMemoryConfiguration(memCfg);

        PersistenceConfiguration pCfg = new PersistenceConfiguration();

        cfg.setCacheConfiguration(
            new CacheConfiguration("partitioned")
            .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(FileWriteAheadLogManager.IGNITE_PDS_WAL_MODE, "LOG_ONLY");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(FileWriteAheadLogManager.IGNITE_PDS_WAL_MODE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /**
     * @throws Exception if failed.
     */
    public void testPageSize_1k() throws Exception {
        checkPageSize(1024);
    }

    /**
     * @throws Exception if failed.
     */
    public void testPageSize_2k() throws Exception {
        checkPageSize(2 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    public void testPageSize_4k() throws Exception {
        checkPageSize(4 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    public void testPageSize_8k() throws Exception {
        checkPageSize(8 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    public void testPageSize_16k() throws Exception {
        checkPageSize(16 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    private void checkPageSize(int pageSize) throws Exception {
        this.pageSize = pageSize;

        IgniteEx ignite = startGrid(0);

        try {
            final IgniteCache<Object, Object> cache = ignite.cache("partitioned");
            final long endTime = System.currentTimeMillis() + 60_000;

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Random rnd = ThreadLocalRandom.current();

                    while (System.currentTimeMillis() < endTime) {
                        for (int i = 0; i < 500; i++)
                            cache.put(rnd.nextInt(100_000), rnd.nextInt());
                    }

                    return null;
                }
            }, 16, "runner");
        }
        finally {
            stopAllGrids();
        }
    }
}
