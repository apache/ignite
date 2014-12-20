/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.transactions.GridCacheTxConcurrency.*;
import static org.apache.ignite.transactions.GridCacheTxIsolation.*;

/**
 * Simple cache test.
 */
public class IgniteTxTimeoutAbstractTest extends GridCommonAbstractTest {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Grid count. */
    private static final int GRID_COUNT = 2;

    /** Grid instances. */
    private static final List<Ignite> IGNITEs = new ArrayList<>();

    /** Transaction timeout. */
    private static final long TIMEOUT = 50;

    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRID_COUNT; i++)
            IGNITEs.add(startGrid(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        IGNITEs.clear();
    }

    /**
     * @param i Grid index.
     * @return Cache.
     */
    @Override protected <K, V> GridCache<K, V> cache(int i) {
        return IGNITEs.get(i).cache(null);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticReadCommitted() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticRepeatableRead() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticSerializable() throws Exception {
        checkTransactionTimeout(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticReadCommitted() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticRepeatableRead() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticSerializable() throws Exception {
        checkTransactionTimeout(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws IgniteCheckedException If test failed.
     */
    private void checkTransactionTimeout(GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation) throws Exception {

        int idx = RAND.nextInt(GRID_COUNT);

        GridCache<Integer, String> cache = cache(idx);

        IgniteTx tx = cache.txStart(concurrency, isolation, TIMEOUT, 0);

        try {
            info("Storing value in cache [key=1, val=1]");

            cache.put(1, "1");

            long sleep = TIMEOUT * 2;

            info("Going to sleep for (ms): " + sleep);

            Thread.sleep(sleep);

            info("Storing value in cache [key=1, val=2]");

            cache.put(1, "2");

            info("Committing transaction: " + tx);

            tx.commit();

            assert false : "Timeout never happened for transaction: " + tx;
        }
        catch (GridCacheTxTimeoutException e) {
            info("Received expected timeout exception [msg=" + e.getMessage() + ", tx=" + tx + ']');
        }
        finally {
            tx.close();
        }
    }
}
