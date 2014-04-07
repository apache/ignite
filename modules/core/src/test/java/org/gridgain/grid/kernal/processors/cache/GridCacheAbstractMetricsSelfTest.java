/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Cache metrics test.
 */
public abstract class GridCacheAbstractMetricsSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int KEY_CNT = 50;

    /** */
    private static final int TX_CNT = 3;

    /** {@inheritDoc} */
    @Override protected GridCacheWriteSynchronizationMode writeSynchronization() {
        return FULL_SYNC;
    }

    /** {@inheritDoc} */
    @Override protected boolean swapEnabled() {
        return false;
    }

    /**
     * @return Key count.
     */
    protected int keyCount() {
        return KEY_CNT;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (int i = 0; i < gridCount(); i++) {
            Grid g = grid(i);

            g.cache(null).removeAll();

            assert g.cache(null).isEmpty();

            g.cache(null).resetMetrics();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testWritesReads() throws Exception {
        GridCache<Integer, Integer> cache0 = grid(0).cache(null);

        int keyCnt = keyCount();

        int expReads = 0;
        int expMisses = 0;

        // Put and get a few keys.
        for (int i = 0; i < keyCnt; i++) {
            cache0.put(i, i); // +1 read

            if (cache0.affinity().isPrimary(grid(0).localNode(), i)) {
                expReads++;
                expMisses++;
            }
            else {
                expReads += 2;
                expMisses += 2;
            }

            info("Writes: " + cache0.metrics().writes());

            for (int j = 0; j < gridCount(); j++) {
                GridCache<Integer, Integer> cache = grid(j).cache(null);

                int cacheWrites = cache.metrics().writes();

                assertEquals("Wrong cache metrics [i=" + i + ", grid=" + j + ']', i + 1, cacheWrites);
            }

            assertEquals("Wrong value for key: " + i, Integer.valueOf(i), cache0.get(i)); // +1 read

            expReads++;
        }

        // Check metrics for the whole cache.
        long writes = 0;
        long reads = 0;
        long hits = 0;
        long misses = 0;

        for (int i = 0; i < gridCount(); i++) {
            GridCacheMetrics m = grid(i).cache(null).metrics();

            writes += m.writes();
            reads += m.reads();
            hits += m.hits();
            misses += m.misses();
        }

        assertEquals(keyCnt * gridCount(), writes);
        assertEquals("Stats [reads=" + reads + ", hits=" + hits + ", misses=" + misses + ']', expReads, reads);
        assertEquals(keyCnt, hits);
        assertEquals(expMisses, misses);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMisses() throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        // TODO: GG-7578.
        if (cache.configuration().getCacheMode() == GridCacheMode.REPLICATED)
            return;

        int keyCnt = keyCount();

        int expReads = 0;

        // Get a few keys missed keys.
        for (int i = 0; i < keyCnt; i++) {
            assertNull("Value is not null for key: " + i, cache.get(i));

            if (cache.affinity().isPrimary(grid(0).localNode(), i))
                expReads++;
            else
                expReads += 2;
        }

        // Check metrics for the whole cache.
        long writes = 0;
        long reads = 0;
        long hits = 0;
        long misses = 0;

        for (int i = 0; i < gridCount(); i++) {
            GridCacheMetrics m = grid(i).cache(null).metrics();

            writes += m.writes();
            reads += m.reads();
            hits += m.hits();
            misses += m.misses();
        }

        assertEquals(0, writes);
        assertEquals(expReads, reads);
        assertEquals(0, hits);
        assertEquals(expReads, misses);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedCommits() throws Exception {
        testCommits(OPTIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedCommitsNoData() throws Exception {
        testCommits(OPTIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadCommits() throws Exception {
        testCommits(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadCommitsNoData() throws Exception {
        testCommits(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableCommits() throws Exception {
        testCommits(OPTIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableCommitsNoData() throws Exception {
        testCommits(OPTIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedCommits() throws Exception {
        testCommits(PESSIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedCommitsNoData() throws Exception {
        testCommits(PESSIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadCommits() throws Exception {
        testCommits(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadCommitsNoData() throws Exception {
        testCommits(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableCommits() throws Exception {
        testCommits(PESSIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableCommitsNoData() throws Exception {
        testCommits(PESSIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedRollbacks() throws Exception {
        testRollbacks(OPTIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedRollbacksNoData() throws Exception {
        testRollbacks(OPTIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadRollbacks() throws Exception {
        testRollbacks(OPTIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadRollbacksNoData() throws Exception {
        testRollbacks(OPTIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableRollbacks() throws Exception {
        testRollbacks(OPTIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableRollbacksNoData() throws Exception {
        testRollbacks(OPTIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedRollbacks() throws Exception {
        testRollbacks(PESSIMISTIC, READ_COMMITTED, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedRollbacksNoData() throws Exception {
        testRollbacks(PESSIMISTIC, READ_COMMITTED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadRollbacks() throws Exception {
        testRollbacks(PESSIMISTIC, REPEATABLE_READ, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadRollbacksNoData() throws Exception {
        testRollbacks(PESSIMISTIC, REPEATABLE_READ, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableRollbacks() throws Exception {
        testRollbacks(PESSIMISTIC, SERIALIZABLE, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableRollbacksNoData() throws Exception {
        testRollbacks(PESSIMISTIC, SERIALIZABLE, false);
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @param put Put some data if {@code true}.
     * @throws Exception If failed.
     */
    private void testCommits(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation, boolean put)
        throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        for (int i = 0; i < TX_CNT; i++) {
            GridCacheTx tx = cache.txStart(concurrency, isolation);

            if (put)
                for (int j = 0; j < keyCount(); j++)
                    cache.put(j, j);

            tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            GridCacheMetrics metrics = grid(i).cache(null).metrics();

            if (i == 0)
                assertEquals(TX_CNT, metrics.txCommits());
            else
                assertEquals(0, metrics.txCommits());

            assertEquals(0, metrics.txRollbacks());
        }
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @param put Put some data if {@code true}.
     * @throws Exception If failed.
     */
    private void testRollbacks(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation,
        boolean put) throws Exception {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        for (int i = 0; i < TX_CNT; i++) {
            GridCacheTx tx = cache.txStart(concurrency ,isolation);

            if (put)
                for (int j = 0; j < keyCount(); j++)
                    cache.put(j, j);

            tx.rollback();
        }

        for (int i = 0; i < gridCount(); i++) {
            GridCacheMetrics metrics = grid(i).cache(null).metrics();

            assertEquals(0, metrics.txCommits());

            if (i == 0)
                assertEquals(TX_CNT, metrics.txRollbacks());
            else
                assertEquals(0, metrics.txRollbacks());
        }
    }
}
