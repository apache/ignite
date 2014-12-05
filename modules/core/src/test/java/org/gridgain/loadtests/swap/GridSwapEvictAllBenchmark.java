/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.swap;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.fifo.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.swapspace.file.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.loadtests.util.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Swap benchmark.
 */
@SuppressWarnings("BusyWait")
public class GridSwapEvictAllBenchmark {
    /** Eviction policy size. */
    public static final int EVICT_PLC_SIZE = 3200000;

    /** Keys count. */
    public static final int KEYS_CNT = 3000000;

    /** Batch size. */
    private static final int BATCH_SIZE = 200;

    /**
     * @param args Parameters.
     * @throws Exception If failed.
     */
    public static void main(String ... args) throws Exception {
        GridFileLock fileLock = GridLoadTestUtils.fileLock();

        fileLock.lock();

        try {
            String outputFileName = args.length > 0 ? args[0] : null;

            Ignite g = start(new GridCacheStoreAdapter<Long, String>() {
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
                    assert false;
                }

                @Override public void remove(@Nullable GridCacheTx tx, Long key) {
                    assert false;
                }
            });

            try {
                GridCache<Object, Object> cache = g.cache(null);

                assert cache != null;

                cache.loadCache(null, 0);

                X.println("Finished load cache.");

                // Warm-up.
                runBenchmark(BATCH_SIZE, BATCH_SIZE, null);

                // Run.
                runBenchmark(KEYS_CNT, BATCH_SIZE, outputFileName);

                assert g.configuration().getSwapSpaceSpi().count(null) == 0;
            }
            finally {
                G.stopAll(false);
            }
        }
        finally {
            fileLock.close();
        }
    }

    /**
     * @param keysCnt Number of keys to swap and promote.
     * @param batchSize Size of batch to swap/promote.
     * @param outputFileName Output file name.
     * @throws Exception If failed.
     */
    private static void runBenchmark(final int keysCnt, int batchSize, @Nullable String outputFileName)
        throws Exception {
        assert keysCnt % batchSize == 0;

        final AtomicInteger evictedKeysCnt = new AtomicInteger();

        final GridCumulativeAverage evictAvg = new GridCumulativeAverage();

        Thread evictCollector = GridLoadTestUtils.startDaemon(new Runnable() {
            @Override public void run() {
                int curCnt = evictedKeysCnt.get();

                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Thread.sleep(1000);

                        int newCnt = evictedKeysCnt.get();

                        int entPerSec = newCnt - curCnt;

                        X.println(">>> Evicting " + entPerSec + " entries/second");

                        evictAvg.update(entPerSec);

                        curCnt = newCnt;
                    }
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                finally {
                    X.println(">>> Average eviction speed: " + evictAvg + " entries/second");
                }
            }
        });

        long start = System.currentTimeMillis();

        GridCache<Object, Object> cache = G.grid().cache(null);

        assert cache != null;

        Collection<Long> keys = new ArrayList<>(batchSize);

        for (long i = 0; i < keysCnt; i++) {
            keys.add(i);

            if (keys.size() == batchSize) {
                cache.evictAll(keys);

                evictedKeysCnt.addAndGet(batchSize);

                keys.clear();
            }
        }

        assert keys.isEmpty();

        long end = System.currentTimeMillis();

        X.println("Done evicting in " + (end - start) + "ms");

        evictCollector.interrupt();

        final AtomicInteger unswappedKeys = new AtomicInteger();

        final GridCumulativeAverage unswapAvg = new GridCumulativeAverage();

        Thread unswapCollector = GridLoadTestUtils.startDaemon(new Runnable() {
            @Override public void run() {
                int curCnt = unswappedKeys.get();

                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Thread.sleep(1000);

                        int newCnt = unswappedKeys.get();

                        int entPerSec = newCnt - curCnt;

                        X.println(">>> Unswapping " + entPerSec + " entries/second");

                        unswapAvg.update(entPerSec);

                        curCnt = newCnt;
                    }
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                finally {
                    X.println(">>> Average unswapping speed: " + unswapAvg + " entries/second");
                }
            }
        });

        start = System.currentTimeMillis();

        for (long i = 0; i < keysCnt; i++) {
            keys.add(i);

            if (keys.size() == batchSize) {
                cache.promoteAll(keys);

                unswappedKeys.addAndGet(batchSize);

                keys.clear();
            }
        }

        assert keys.isEmpty();

        end = System.currentTimeMillis();

        X.println("Done promote in " + (end - start) + "ms");

        unswapCollector.interrupt();

        if (outputFileName != null)
            GridLoadTestUtils.appendLineToFile(
                outputFileName,
                "%s,%d,%d",
                GridLoadTestUtils.DATE_TIME_FORMAT.format(new Date()),
                evictAvg.get(),
                unswapAvg.get()
            );
    }

    /**
     * @param store Cache store.
     * @return Started grid.
     * @throws GridException If failed.
     */
    private static Ignite start(GridCacheStore<Long, String> store) throws GridException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        GridTcpDiscoveryIpFinder finder = new GridTcpDiscoveryVmIpFinder(true);

        disco.setIpFinder(finder);

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setSwapEnabled(true);
        ccfg.setEvictSynchronized(false);
        ccfg.setEvictionPolicy(new GridCacheFifoEvictionPolicy(EVICT_PLC_SIZE));
        ccfg.setStore(store);

        GridFileSwapSpaceSpi swap = new GridFileSwapSpaceSpi();

//        swap.setConcurrencyLevel(16);
//        swap.setWriterThreadsCount(16);

//        swap.setLevelDbCacheSize(128 * 1024 * 1024);
//        swap.setLevelDbWriteBufferSize(128 * 1024 * 1024);
//        swap.setLevelDbBlockSize(1024 * 1024);
//        swap.setLevelDbParanoidChecks(false);
//        swap.setLevelDbVerifyChecksums(false);

        cfg.setSwapSpaceSpi(swap);

        ccfg.setCacheMode(GridCacheMode.LOCAL);
        ccfg.setQueryIndexEnabled(false);

        cfg.setCacheConfiguration(ccfg);

        return G.start(cfg);
    }

}
