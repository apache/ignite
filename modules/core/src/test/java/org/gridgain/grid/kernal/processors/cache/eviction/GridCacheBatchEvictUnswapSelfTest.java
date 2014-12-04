package org.gridgain.grid.kernal.processors.cache.eviction;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.fifo.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Swap benchmark.
 */
@SuppressWarnings("BusyWait")
public class GridCacheBatchEvictUnswapSelfTest extends GridCacheAbstractSelfTest {
    /** Eviction policy size. */
    public static final int EVICT_PLC_SIZE = 100000;

    /** Keys count. */
    public static final int KEYS_CNT = 100000;

    /** Batch size. */
    private static final int BATCH_SIZE = 200;

    /** Number of threads for concurrent benchmark + concurrency level. */
    private static final int N_THREADS = 8;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        // Let this test run 2 minutes as it runs for 20 seconds locally.
        return 2 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cacheCfg = super.cacheConfiguration(gridName);

        cacheCfg.setCacheMode(GridCacheMode.PARTITIONED);
        cacheCfg.setStore(new GridCacheStoreAdapter<Long, String>() {
            @Nullable @Override public String load(@Nullable GridCacheTx tx, Long key) {
                return null;
            }

            @Override public void loadCache(final IgniteBiInClosure<Long, String> c,
                @Nullable Object... args) {
                for (int i = 0; i < KEYS_CNT; i++)
                    c.apply((long)i, String.valueOf(i));
            }

            @Override public void put(@Nullable GridCacheTx tx, Long key,
                @Nullable String val) {
            }

            @Override public void remove(@Nullable GridCacheTx tx, Long key) {
            }
        });

        cacheCfg.setEvictionPolicy(new GridCacheFifoEvictionPolicy(EVICT_PLC_SIZE));
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setEvictSynchronized(false);
        cacheCfg.setDistributionMode(PARTITIONED_ONLY);

        return cacheCfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentEvictions() throws Exception {
        runConcurrentTest(grid(0), KEYS_CNT, BATCH_SIZE);
    }

    /**
     * @param g Grid instance.
     * @param keysCnt Number of keys to swap and promote.
     * @param batchSize Size of batch to swap/promote.
     * @throws Exception If failed.
     */
    private void runConcurrentTest(Ignite g, final int keysCnt, final int batchSize) throws Exception {
        assert keysCnt % batchSize == 0;

        final AtomicInteger evictedKeysCnt = new AtomicInteger();

        final GridCache<Object, Object> cache = g.cache(null);

        cache.loadCache(null, 0);

        info("Finished load cache.");

        IgniteFuture<?> evictFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                Collection<Long> keys = new ArrayList<>(batchSize);

                int evictedBatches = 0;

                for (long i = 0; i < keysCnt; i++) {
                    keys.add(i);

                    if (keys.size() == batchSize) {
                        cache.evictAll(keys);

                        evictedKeysCnt.addAndGet(batchSize);

                        keys.clear();

                        evictedBatches++;

                        if (evictedBatches % 100 == 0 && evictedBatches > 0)
                            info("Evicted " + (evictedBatches * batchSize) + " entries.");
                    }
                }
            }
        }, N_THREADS, "evict");

        final AtomicInteger unswappedKeys = new AtomicInteger();

        IgniteFuture<?> unswapFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    Collection<Long> keys = new ArrayList<>(batchSize);

                    int unswappedBatches = 0;

                    for (long i = 0; i < keysCnt; i++) {
                        keys.add(i);

                        if (keys.size() == batchSize) {
                            cache.promoteAll(keys);

                            unswappedKeys.addAndGet(batchSize);

                            keys.clear();

                            unswappedBatches++;

                            if (unswappedBatches % 100 == 0 && unswappedBatches > 0)
                                info("Unswapped " + (unswappedBatches * batchSize) + " entries.");
                        }
                    }
                }
                catch (GridException e) {
                    e.printStackTrace();
                }
            }
        }, N_THREADS, "promote");

        evictFut.get();

        unswapFut.get();

        info("Clearing cache.");

        for (long i = 0; i < KEYS_CNT; i++)
            cache.remove(i);
    }
}
