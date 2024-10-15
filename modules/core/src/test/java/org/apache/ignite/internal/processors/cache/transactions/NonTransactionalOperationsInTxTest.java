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

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Checks how non transactional cache operations work within a transaction.
 */
public class NonTransactionalOperationsInTxTest extends GridCommonAbstractTest {
    /** */
    protected static IgniteEx ignite;

    /** */
    protected static IgniteEx igniteClient;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setName(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    protected void startCluster() throws Exception {
        ignite = startGrid("srv0");
        igniteClient = startClientGrid("cli0");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startCluster();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests that a non-transactional cache operation {@link IgniteCache#clear()} is not allowed within a transaction.
     */
    @Test
    public void testIgniteCacheNonTransactionalOperations() {
        checkServerNodeNonTxOperation(IgniteCache::clear);

        checkClientNodeNonTxOperation(IgniteCache::clear);
    }

    /**
     * Otherwise - it should throw exception.
     * @param op Operation.
     */
    private void checkServerNodeNonTxOperation(Consumer<IgniteCache<Object, Object>> op) {
        IgniteCache<Object, Object> srvNodeCache = ignite.createCache(new CacheConfiguration<>("my-cache")
            .setAtomicityMode(TRANSACTIONAL));

        srvNodeCache.put(1, 1);

        IgniteException err = null;

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            srvNodeCache.put(2, 2);

            op.accept(srvNodeCache);

            tx.commit();
        }
        catch (IgniteException e) {
            err = e;
        }

        assertTrue(err != null && err.getMessage().startsWith("Failed to invoke a non-transactional operation" +
            " within a transaction"));

        assertTrue(srvNodeCache.containsKey(1));

        assertFalse(srvNodeCache.containsKey(2));

        srvNodeCache.destroy();
    }

    /** */
    @Test
    public void testClientNodeNonTxOperation() {
        IgniteCache<Object, Object> clientNodeCache = igniteClient.createCache(new CacheConfiguration<>("my-cache")
            .setAtomicityMode(TRANSACTIONAL));

        clientNodeCache.put(1, 1);

        IgniteException err = null;

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            clientNodeCache.put(2, 2);

            clientNodeCache.clear();

            tx.commit();
        }
        catch (IgniteException e) {
            err = e;
        }

        assertTrue(err != null && err.getMessage().startsWith("Failed to invoke a non-transactional operation" +
            " within a transaction"));

        assertTrue(clientNodeCache.containsKey(1));

        assertFalse(clientNodeCache.containsKey(2));

        ignite.cache("my-cache").destroy();
    }

    /**
     * Otherwise - it should throw exception.
     * @param op Operation.
     */
    private void checkClientNodeNonTxOperation(Consumer<IgniteCache<Object, Object>> op) {
        IgniteCache<Object, Object> clientNodeCache = igniteClient.createCache(new CacheConfiguration<>("my-cache")
            .setAtomicityMode(TRANSACTIONAL));

        clientNodeCache.put(1, 1);

        IgniteException err = null;

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            clientNodeCache.put(2, 2);

            op.accept(clientNodeCache);

            tx.commit();
        }
        catch (IgniteException e) {
            err = e;
        }

        assertTrue(err != null && err.getMessage().startsWith("Failed to invoke a non-transactional operation" +
            " within a transaction"));

        assertTrue(clientNodeCache.containsKey(1));

        assertFalse(clientNodeCache.containsKey(2));

        ignite.cache("my-cache").destroy();
    }
}
