/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.transactions.GridCacheTxConcurrency.*;

/**
 * Tests transaction during cache preloading.
 */
public abstract class GridCacheTxPreloadAbstractTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 6;

    /** */
    private static volatile boolean keyNotLoaded;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        keyNotLoaded = false;

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteTxPreloading() throws Exception {
        GridCache<String, Integer> cache = cache(0);

        for (int i = 0; i < 10000; i++)
            cache.put(String.valueOf(i), 0);

        final AtomicInteger gridIdx = new AtomicInteger(1);

        IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    int idx = gridIdx.getAndIncrement();

                    startGrid(idx);

                    return null;
                }
            },
            GRID_CNT - 1,
            "grid-starter-" + getName()
        );

        waitForRemoteNodes(grid(0), 2);

        Set<String> keys = new HashSet<>();

        for (int i = 0; i < 10; i++)
            keys.add(String.valueOf(i * 1000));

        cache.transformAll(keys, new C1<Integer, Integer>() {
            @Override public Integer apply(Integer val) {
                if (val == null)
                    keyNotLoaded = true;

                return val + 1;
            }
        });

        assertFalse(keyNotLoaded);

        fut.get();

        for (int i = 0; i < GRID_CNT; i++)
            // Wait for preloader.
            cache(i).forceRepartition().get();

        for (int i = 0; i < GRID_CNT; i++) {
            for (String key : keys)
                assertEquals("Unexpected value for cache " + i, (Integer)1, cache(i).get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalTxPreloadingOptimistic() throws Exception {
        testLocalTxPreloading(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalTxPreloadingPessimistic() throws Exception {
        testLocalTxPreloading(PESSIMISTIC);
    }

    /**
     * Tries to execute transaction doing transform when target key is not yet preloaded.
     *
     * @param txConcurrency Transaction concurrency;
     * @throws Exception If failed.
     */
    private void testLocalTxPreloading(GridCacheTxConcurrency txConcurrency) throws Exception {
        Map<String, Integer> map = new HashMap<>();

        for (int i = 0; i < 10000; i++)
            map.put(String.valueOf(i), 0);

        GridCache<String, Integer> cache0 = cache(0);

        cache0.putAll(map);

        final String TX_KEY = "9000";

        int expVal = 0;

        for (int i = 1; i < GRID_CNT; i++) {
            assertEquals((Integer)expVal, cache0.get(TX_KEY));

            startGrid(i);

            GridCache<String, Integer> cache = cache(i);

            try (GridCacheTx tx = cache.txStart(txConcurrency, GridCacheTxIsolation.READ_COMMITTED)) {
                cache.transform(TX_KEY, new C1<Integer, Integer>() {
                    @Override public Integer apply(Integer val) {
                        if (val == null) {
                            keyNotLoaded = true;

                            return 1;
                        }

                        return val + 1;
                    }
                });

                tx.commit();
            }

            assertFalse(keyNotLoaded);

            expVal++;

            assertEquals((Integer)expVal, cache.get(TX_KEY));
        }

        for (int i = 0; i < GRID_CNT; i++)
            assertEquals("Unexpected value for cache " + i, (Integer)expVal, cache(i).get(TX_KEY));
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setPreloadMode(GridCachePreloadMode.ASYNC);

        cfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setStore(null);

        return cfg;
    }
}
