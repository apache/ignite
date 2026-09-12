package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCachePartitionedPrimaryNodeFailureRecoveryWithReadsTest extends GridCommonAbstractTest {
    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<?, ?> cacheConfiguration() {
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(2);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        return ccfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testRecoveryWithReadTransactionExistOnlyOnPrimary() throws Exception {
        startGridsMultiThreaded(3);

        try {
            Ignite failed = G.allGrids().get(0);
            Ignite backup = G.allGrids().get(1);

            Integer failedKey = primaryKey(failed.getOrCreateCache(DEFAULT_CACHE_NAME));
            Integer backupKey = primaryKey(backup.getOrCreateCache(DEFAULT_CACHE_NAME));

            multithreadedAsync(() -> {
                try {
                    Transaction tx = failed.transactions().txStart();

                    IgniteCache<Integer, Integer> cache = failed.getOrCreateCache(DEFAULT_CACHE_NAME);

                    cache.put(failedKey, failedKey);

                    cache.get(backupKey);

                    // failed and backup nodes have 2 transactions (read + write), while backup #2 have only write tx.
                    ((TransactionProxyImpl<?, ?>)tx).tx().prepare(true);
                }
                catch (Exception e) {
                    fail("Should not happen [exception=" + e + "]");
                }
            }, 1).get();

            failed.close();

            assertTrue(GridTestUtils.waitForCondition(() -> {
                for (Ignite ignite : G.allGrids()) {
                    IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

                    if (cache.get(failedKey) == null)
                        return false;
                }

                return true;
            }, 5000));
        }
        finally {
            stopAllGrids();
        }
    }
}
