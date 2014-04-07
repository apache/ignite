/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.swapspace.file.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Tests for byte array values in distributed caches.
 */
public abstract class GridCacheAbstractDistributedByteArrayValuesSelfTest extends
    GridCacheAbstractByteArrayValuesSelfTest {
    /** Grids. */
    protected static Grid[] grids;

    /** Regular caches. */
    private static GridCache<Integer, Object>[] caches;

    /** Offheap caches. */
    private static GridCache<Integer, Object>[] cachesOffheap;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        c.setCacheConfiguration(cacheConfiguration(), offheapCacheConfiguration());
        c.setSwapSpaceSpi(new GridFileSwapSpaceSpi());
        c.setPeerClassLoadingEnabled(peerClassLoading());

        return c;
    }

    /**
     * @return Whether peer class loading is enabled.
     */
    protected abstract boolean peerClassLoading();

    /**
     * @return How many grids to start.
     */
    protected int gridCount() {
        return 3;
    }

    /**
     * @return Cache configuration.
     */
    protected GridCacheConfiguration cacheConfiguration() {
        GridCacheConfiguration cfg = cacheConfiguration0();

        cfg.setName(CACHE_REGULAR);

        return cfg;
    }

    /**
     * @return Internal cache configuration.
     */
    protected abstract GridCacheConfiguration cacheConfiguration0();

    /**
     * @return Offheap cache configuration.
     */
    protected GridCacheConfiguration offheapCacheConfiguration() {
        GridCacheConfiguration cfg = offheapCacheConfiguration0();

        cfg.setName(CACHE_OFFHEAP);

        return cfg;
    }

    /**
     * @return Internal offheap cache configuration.
     */
    protected abstract GridCacheConfiguration offheapCacheConfiguration0();

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        int gridCnt = gridCount();

        assert gridCnt > 0;

        grids = new Grid[gridCnt];
        caches = new GridCache[gridCnt];
        cachesOffheap = new GridCache[gridCnt];

        for (int i = 0; i < gridCnt; i++) {
            grids[i] = startGrid(i);

            caches[i] = grids[i].cache(CACHE_REGULAR);
            cachesOffheap[i] = grids[i].cache(CACHE_OFFHEAP);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        caches = null;
        cachesOffheap = null;

        grids = null;
    }

    /**
     * Check whether cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testPessimistic() throws Exception {
        testTransaction0(caches, PESSIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticMixed() throws Exception {
        testTransactionMixed0(caches, PESSIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Check whether offheap cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticOffheap() throws Exception {
        testTransaction0(cachesOffheap, PESSIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether offheap cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticOffheapMixed() throws Exception {
        testTransactionMixed0(cachesOffheap, PESSIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Check whether cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testOptimistic() throws Exception {
        testTransaction0(caches, OPTIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticMixed() throws Exception {
        testTransactionMixed0(caches, OPTIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Check whether offheap cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticOffheap() throws Exception {
        testTransaction0(cachesOffheap, OPTIMISTIC, KEY_1, wrap(1));
    }

    /**
     * Check whether offheap cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticOffheapMixed() throws Exception {
        testTransactionMixed0(cachesOffheap, OPTIMISTIC, KEY_1, wrap(1), KEY_2, 1);
    }

    /**
     * Test swapping.
     *
     * @throws Exception If failed.
     */
    public void testSwap() throws Exception {
        for (GridCache<Integer, Object> cache : caches)
            assert cache.configuration().isSwapEnabled();

        byte[] val1 = wrap(1);

        GridCache<Integer, Object> primaryCache = null;

        for (GridCache<Integer, Object> cache : caches) {
            if (cache.entry(KEY_1).primary()) {
                primaryCache = cache;

                break;
            }
        }

        assert primaryCache != null;

        primaryCache.put(KEY_1, val1);

        assert Arrays.equals(val1, (byte[])primaryCache.get(1));

        assert primaryCache.evict(1);

        assert primaryCache.peek(1) == null;

        assert Arrays.equals(val1, (byte[])primaryCache.promote(1));
    }

    /**
     * Test transaction behavior.
     *
     * @param caches Caches.
     * @param concurrency Concurrency.
     * @param key Key.
     * @param val Value.
     * @throws Exception If failed.
     */
    private void testTransaction0(GridCache<Integer, Object>[] caches, GridCacheTxConcurrency concurrency,
        Integer key, byte[] val) throws Exception {
        testTransactionMixed0(caches, concurrency, key, val, null, null);
    }

    /**
     * Test transaction behavior.
     *
     * @param caches Caches.
     * @param concurrency Concurrency.
     * @param key1 Key 1.
     * @param val1 Value 1.
     * @param key2 Key 2.
     * @param val2 Value 2.
     * @throws Exception If failed.
     */
    private void testTransactionMixed0(GridCache<Integer, Object>[] caches, GridCacheTxConcurrency concurrency,
        Integer key1, byte[] val1, @Nullable Integer key2, @Nullable Object val2) throws Exception {
        for (GridCache<Integer, Object> cache : caches) {
            GridCacheTx tx = cache.txStart(concurrency, REPEATABLE_READ);

            try {
                cache.put(key1, val1);

                if (key2 != null)
                    cache.put(key2, val2);

                tx.commit();
            }
            finally {
                tx.close();
            }

            for (GridCache<Integer, Object> cacheInner : caches) {
                tx = cacheInner.txStart(concurrency, REPEATABLE_READ);

                try {
                    assert Arrays.equals(val1, (byte[])cacheInner.get(key1));

                    if (key2 != null) {
                        Object actual = cacheInner.get(key2);

                        assert F.eq(val2, actual) : "Invalid result [val2=" + val2 + ", actual=" + actual + ']';
                    }

                    tx.commit();
                }
                finally {
                    tx.close();
                }
            }

            tx = cache.txStart(concurrency, REPEATABLE_READ);

            try {
                cache.remove(key1);

                if (key2 != null)
                    cache.remove(key2);

                tx.commit();
            }
            finally {
                tx.close();
            }
        }
    }
}
