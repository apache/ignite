package org.apache.ignite.internal.processors.cache.transactions;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;

/**
 * Test scenario when entries are updates using non tx counter assignment.
 */
public class TxPartitionCounterStateNonTxTest extends GridCommonAbstractTest {
    /** */
    private static final String ATOMIC_CACHE = "atomic";

    /** */
    private static final String TX_CACHE = "tx";

    /** */
    private static final int PARTITION_ID = 0;

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private boolean persistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setWalSegmentSize(8 * MB).setWalMode(LOG_ONLY).setPageSize(1024).setCheckpointFrequency(10000000000L).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistenceEnabled).
                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        cfg.setCacheConfiguration(
            new CacheConfiguration(TX_CACHE).setCacheMode(PARTITIONED).setAtomicityMode(TRANSACTIONAL),
            new CacheConfiguration(ATOMIC_CACHE).setCacheMode(PARTITIONED).setAtomicityMode(ATOMIC));

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (persistenceEnabled)
            cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (persistenceEnabled)
            cleanPersistenceDir();
    }

    public void testAtomic() throws Exception {
        try {
            Ignite ignite = startGridsMultiThreaded(3);

            List<Integer> loaded = loadDataToPartition(PARTITION_ID, ignite.name(), ATOMIC_CACHE, 1, 0);

            System.out.println();
        }
        finally {
            stopAllGrids();
        }
    }
}
