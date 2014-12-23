/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;

import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests value read inside transaction.
 */
public abstract class GridCacheAbstractTxReadTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.getTransactionsConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setStore(null);

        return cfg;
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadOptimisticReadCommitted() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.OPTIMISTIC, IgniteTxIsolation.READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadOptimisticRepeatableRead() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.OPTIMISTIC, IgniteTxIsolation.REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadOptimisticSerializable() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.OPTIMISTIC, IgniteTxIsolation.SERIALIZABLE);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadPessimisticReadCommitted() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.PESSIMISTIC, IgniteTxIsolation.READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadPessimisticRepeatableRead() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.PESSIMISTIC, IgniteTxIsolation.REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If failed
     */
    public void testTxReadPessimisticSerializable() throws IgniteCheckedException {
        checkTransactionalRead(IgniteTxConcurrency.PESSIMISTIC, IgniteTxIsolation.SERIALIZABLE);
    }

    /**
     * Tests sequential value write and read inside transaction.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws IgniteCheckedException If failed
     */
    protected void checkTransactionalRead(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation)
        throws IgniteCheckedException {
        GridCache<String, Integer> cache = cache(0);

        cache.clearAll();

        IgniteTx tx = cache.txStart(concurrency, isolation);

        try {
            cache.put("key", 1);

            assertEquals("Invalid value after put", 1, cache.get("key").intValue());

            tx.commit();
        }
        finally {
            tx.close();
        }

        assertEquals("Invalid cache size after put", 1, cache.size());

        try {
            tx = cache.txStart(concurrency, isolation);

            assertEquals("Invalid value inside transactional read", Integer.valueOf(1), cache.get("key"));

            tx.commit();
        }
        finally {
            tx.close();
        }
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }
}
