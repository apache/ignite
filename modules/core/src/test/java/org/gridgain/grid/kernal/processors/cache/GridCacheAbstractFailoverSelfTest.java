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
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Failover tests for cache.
 */
public abstract class GridCacheAbstractFailoverSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final long TEST_TIMEOUT = 3 * 60 * 1000;

    /** */
    private static final String NEW_GRID_NAME = "newGrid";

    /** */
    private static final int ENTRY_CNT = 100;

    /** */
    private static final int TOP_CHANGE_CNT = 5;

    /** */
    private static final int TOP_CHANGE_THREAD_CNT = 3;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.getTransactionsConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setPreloadMode(SYNC);
        cfg.setDgcFrequency(0);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(gridCount());

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopologyChange() throws Exception {
        testTopologyChange(null, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConstantTopologyChange() throws Exception {
        testConstantTopologyChange(null, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws Exception If failed.
     */
    private void testTopologyChange(@Nullable GridCacheTxConcurrency concurrency,
        @Nullable GridCacheTxIsolation isolation) throws Exception {
        boolean tx = concurrency != null && isolation != null;

        if (tx)
            put(cache(), ENTRY_CNT, concurrency, isolation);
        else
            put(cache(), ENTRY_CNT);

        Ignite g = startGrid(NEW_GRID_NAME);

        check(cache(g), ENTRY_CNT);

        int half = ENTRY_CNT / 2;

        if (tx) {
            remove(cache(g), half, concurrency, isolation);
            put(cache(g), half, concurrency, isolation);
        }
        else {
            remove(cache(g), half);
            put(cache(g), half);
        }

        stopGrid(NEW_GRID_NAME);

        check(cache(), ENTRY_CNT);
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws Exception If failed.
     */
    private void testConstantTopologyChange(@Nullable final GridCacheTxConcurrency concurrency,
        @Nullable final GridCacheTxIsolation isolation) throws Exception {
        final boolean tx = concurrency != null && isolation != null;

        if (tx)
            put(cache(), ENTRY_CNT, concurrency, isolation);
        else
            put(cache(), ENTRY_CNT);

        check(cache(), ENTRY_CNT);

        final int half = ENTRY_CNT / 2;

        GridFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
            @Override public void apply() {
                info("Run topology change.");

                try {
                    for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                        info("Topology change " + i);

                        String name = UUID.randomUUID().toString();

                        try {
                            final Ignite g = startGrid(name);

                            for (int k = half; k < ENTRY_CNT; k++)
                                assertNotNull("Failed to get key: 'key" + k + "'", cache(g).get("key" + k));
                        }
                        finally {
                            G.stop(name, false);
                        }
                    }
                }
                catch (Exception e) {
                    throw F.wrap(e);
                }
            }
        }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

        while (!fut.isDone()) {
            if (tx) {
                remove(cache(), half, concurrency, isolation);
                put(cache(), half, concurrency, isolation);
            }
            else {
                remove(cache(), half);
                put(cache(), half);
            }
        }

        fut.get();
    }

    /**
     * @param cache Cache.
     * @param cnt Entry count.
     * @throws GridException If failed.
     */
    private void put(GridCacheProjection<String, Integer> cache, int cnt) throws Exception {
        try {
            for (int i = 0; i < cnt; i++)
                assertTrue("Failed to put key: 'key" + i + "'",  cache.putx("key" + i, i));
        }
        catch (GridException e) {
            // It is ok to fail with topology exception.
            if (!X.hasCause(e, GridTopologyException.class))
                throw e;
        }
    }

    /**
     * @param cache Cache.
     * @param cnt Entry count.
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws GridException If failed.
     */
    private void put(GridCacheProjection<String, Integer> cache, final int cnt,
        GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) throws Exception {
        try {
            info("Putting values to cache [0," + cnt + ')');

            CU.inTx(cache, concurrency, isolation, new CIX1<GridCacheProjection<String, Integer>>() {
                @Override public void applyx(GridCacheProjection<String, Integer> cache)
                    throws GridException {
                    for (int i = 0; i < cnt; i++)
                        assertTrue("Failed to put key: 'key" + i + "'", cache.putx("key" + i, i));
                }
            });
        }
        catch (GridException e) {
            // It is ok to fail with topology exception.
            if (!X.hasCause(e, GridTopologyException.class))
                throw e;
            else
                info("Failed to put values to cache due to topology exception [0," + cnt + ')');
        }
    }

    /**
     * @param cache Cache.
     * @param cnt Entry count.
     * @throws GridException If failed.
     */
    private void remove(GridCacheProjection<String, Integer> cache, int cnt) throws Exception {
        try {
            for (int i = 0; i < cnt; i++)
                cache.removex("key" + i);
        }
        catch (GridException e) {
            // It is ok to fail with topology exception.
            if (!X.hasCause(e, GridTopologyException.class))
                throw e;
        }
    }

    /**
     * @param cache Cache.
     * @param cnt Entry count.
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws GridException If failed.
     */
    private void remove(GridCacheProjection<String, Integer> cache, final int cnt,
        GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) throws Exception {
        try {
            info("Removing values form cache [0," + cnt + ')');

            CU.inTx(cache, concurrency, isolation, new CIX1<GridCacheProjection<String, Integer>>() {
                @Override public void applyx(GridCacheProjection<String, Integer> cache)
                    throws GridException {
                    for (int i = 0; i < cnt; i++)
                        cache.removex("key" + i);
                }
            });
        }
        catch (GridException e) {
            // It is ok to fail with topology exception.
            if (!X.hasCause(e, GridTopologyException.class))
                throw e;
            else
                info("Failed to remove values from cache due to topology exception [0," + cnt + ')');
        }
    }

    /**
     * @param cache Cache.
     * @param expSize Minimum expected cache size.
     * @throws GridException If failed.
     */
    private void check(GridCacheProjection<String,Integer> cache, int expSize) throws GridException {
        int size;

        if (cacheMode() == PARTITIONED) {
            Collection<Integer> res = compute(cache.gridProjection()).broadcast(new GridCallable<Integer>() {
                @GridInstanceResource
                private Ignite g;

                @Override public Integer call() {
                    return cache(g).projection(F.<String, Integer>cachePrimary()).size();
                }
            });

            size = 0 ;

            for (Integer size0 : res)
                size += size0;
        }
        else
            size = cache.size();

        assertTrue("Key set size is lesser then the expected size [size=" + size + ", expSize=" + expSize + ']',
            size >= expSize);

        for (int i = 0; i < expSize; i++)
            assertNotNull("Failed to get value for key: 'key" + i + "'", cache.get("key" + i));
    }

    /**
     * @param g Grid.
     * @return Cache.
     */
    private GridCacheProjection<String,Integer> cache(Ignite g) {
        return g.cache(null);
    }
}
