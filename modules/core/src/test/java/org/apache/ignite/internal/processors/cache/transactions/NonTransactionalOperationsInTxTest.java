package org.apache.ignite.internal.processors.cache.transactions;

import java.util.function.Consumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** Checks how non-transactional cache operations work within a transaction. */
public class NonTransactionalOperationsInTxTest extends GridCommonAbstractTest {
    /** */
    protected static IgniteEx srv;

    /** */
    protected static IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        srv = startGrid("srv0");
        client = startClientGrid("cli0");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests that a non-transactional cache operation {@link IgniteCache#clear()} is not allowed within a transaction
     * and the operation works as expected.
     */
    @Test
    public void testIgniteCacheNonTransactionalOperations() {
        checkNonTxOperation(srv, IgniteCache::clear);

        checkNonTxOperation(client, IgniteCache::clear);
    }

    /**
     * It should throw exception.
     * @param op Operation.
     */
    private void checkNonTxOperation(IgniteEx ignite, Consumer<IgniteCache<Object, Object>> op) {
        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>("my-cache")
            .setAtomicityMode(TRANSACTIONAL));

        cache.put(1, 1);

        IgniteException err = null;

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            cache.put(2, 2);

            op.accept(cache);

            tx.commit();
        }
        catch (IgniteException e) {
            err = e;
        }

        assertTrue(err != null && err.getMessage().startsWith("Failed to invoke a non-transactional operation " +
            "within a transaction"));

        assertTrue(cache.containsKey(1));

        assertFalse(cache.containsKey(2));

        srv.cache("my-cache").destroy();
    }
}
