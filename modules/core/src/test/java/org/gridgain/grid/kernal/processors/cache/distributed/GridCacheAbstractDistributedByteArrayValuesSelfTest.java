/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.transactions.GridCacheTxConcurrency.*;
import static org.apache.ignite.transactions.GridCacheTxIsolation.*;
import static org.junit.Assert.*;

/**
 * Tests for byte array values in distributed caches.
 */
public abstract class GridCacheAbstractDistributedByteArrayValuesSelfTest extends
    GridCacheAbstractByteArrayValuesSelfTest {
    /** Grids. */
    protected static Ignite[] ignites;

    /** Regular caches. */
    private static GridCache<Integer, Object>[] caches;

    /** Offheap values caches. */
    private static GridCache<Integer, Object>[] cachesOffheap;

    /** Offheap tiered caches. */
    private static GridCache<Integer, Object>[] cachesOffheapTiered;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setCacheConfiguration(cacheConfiguration(),
            offheapCacheConfiguration(),
            offheapTieredCacheConfiguration());

        c.setSwapSpaceSpi(new FileSwapSpaceSpi());

        c.setPeerClassLoadingEnabled(peerClassLoading());

        return c;
    }

    /**
     * @return Whether peer class loading is enabled.
     */
    protected abstract boolean peerClassLoading();

    /**
     * @return Whether portable mode is enabled.
     */
    protected boolean portableEnabled() {
        return false;
    }

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
        cfg.setPortableEnabled(portableEnabled());

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
        cfg.setPortableEnabled(portableEnabled());

        return cfg;
    }

    /**
     * @return Offheap tiered cache configuration.
     */
    protected GridCacheConfiguration offheapTieredCacheConfiguration() {
        GridCacheConfiguration cfg = offheapTieredCacheConfiguration0();

        cfg.setName(CACHE_OFFHEAP_TIERED);
        cfg.setPortableEnabled(portableEnabled());

        return cfg;
    }

    /**
     * @return Internal offheap cache configuration.
     */
    protected abstract GridCacheConfiguration offheapCacheConfiguration0();

    /**
     * @return Internal offheap cache configuration.
     */
    protected abstract GridCacheConfiguration offheapTieredCacheConfiguration0();

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        int gridCnt = gridCount();

        assert gridCnt > 0;

        ignites = new Ignite[gridCnt];

        caches = new GridCache[gridCnt];
        cachesOffheap = new GridCache[gridCnt];
        cachesOffheapTiered = new GridCache[gridCnt];

        for (int i = 0; i < gridCnt; i++) {
            ignites[i] = startGrid(i);

            caches[i] = ignites[i].cache(CACHE_REGULAR);
            cachesOffheap[i] = ignites[i].cache(CACHE_OFFHEAP);
            cachesOffheapTiered[i] = ignites[i].cache(CACHE_OFFHEAP_TIERED);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        caches = null;
        cachesOffheap = null;
        cachesOffheapTiered = null;

        ignites = null;
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
    public void testPessimisticOffheapTiered() throws Exception {
        testTransaction0(cachesOffheapTiered, PESSIMISTIC, KEY_1, wrap(1));
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
     * Check whether offheap cache with byte array entry works correctly in PESSIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticOffheapTieredMixed() throws Exception {
        testTransactionMixed0(cachesOffheapTiered, PESSIMISTIC, KEY_1, wrap(1), KEY_2, 1);
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
    public void testOptimisticOffheapTiered() throws Exception {
        testTransaction0(cachesOffheapTiered, OPTIMISTIC, KEY_1, wrap(1));
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
     * Check whether offheap cache with byte array entry works correctly in OPTIMISTIC transaction.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticOffheapTieredMixed() throws Exception {
        testTransactionMixed0(cachesOffheapTiered, OPTIMISTIC, KEY_1, wrap(1), KEY_2, 1);
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
            if (cache.entry(SWAP_TEST_KEY).primary()) {
                primaryCache = cache;

                break;
            }
        }

        assert primaryCache != null;

        primaryCache.put(SWAP_TEST_KEY, val1);

        assert Arrays.equals(val1, (byte[])primaryCache.get(SWAP_TEST_KEY));

        assert primaryCache.evict(SWAP_TEST_KEY);

        assert primaryCache.peek(SWAP_TEST_KEY) == null;

        assert Arrays.equals(val1, (byte[])primaryCache.promote(SWAP_TEST_KEY));
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
            IgniteTx tx = cache.txStart(concurrency, REPEATABLE_READ);

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
                    assertArrayEquals(val1, (byte[])cacheInner.get(key1));

                    if (key2 != null) {
                        Object actual = cacheInner.get(key2);

                        assertEquals(val2, actual);
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

            assertNull(cache.get(key1));
        }
    }
}
