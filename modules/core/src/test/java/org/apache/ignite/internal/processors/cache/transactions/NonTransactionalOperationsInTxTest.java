package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** Checks that non-transactional cache operations fail within a transaction. */
public class NonTransactionalOperationsInTxTest extends GridCommonAbstractTest {
    /** */
    protected CacheConfiguration<Object, Object> ccfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ccfg = new CacheConfiguration<>("my-cache").setAtomicityMode(TRANSACTIONAL);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testIgniteCacheClear() throws Exception {
        startGrid(0);

        checkIgniteCacheClear(grid(0));
    }

    /** */
    @Test
    public void testIgniteCacheClearOnClientNode() throws Exception {
        startGrid(0);

        startClientGrid(1);

        checkIgniteCacheClear(grid(1));
    }

    /**
     * It should throw exception.
     * @param ignite Ignite.
     */
    private void checkIgniteCacheClear(IgniteEx ignite) {
        IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

        cache.put(1, 1);

        assertThrows(null,
                () -> doInTransaction(ignite, () -> {
                    cache.put(2, 2);

                    cache.clear();

                    return null;
                }),
                IgniteException.class,
                "Failed to invoke a non-transactional operation within a transaction"
        );

        assertTrue(cache.containsKey(1));

        assertFalse(cache.containsKey(2));
    }
}
