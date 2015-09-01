/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.loadtests.swap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.loadtests.util.GridCumulativeAverage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.GridFileLock;
import org.apache.ignite.testframework.GridLoadTestUtils;
import org.jetbrains.annotations.Nullable;

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

            Ignite g = start(new CacheStoreAdapter<Long, String>() {
                @Nullable @Override public String load(Long key) {
                    return null;
                }

                @Override public void loadCache(final IgniteBiInClosure<Long, String> c,
                    @Nullable Object... args) {
                    for (int i = 0; i < KEYS_CNT; i++)
                        c.apply((long)i, String.valueOf(i));
                }

                @Override public void write(javax.cache.Cache.Entry<? extends Long, ? extends String> e) {
                    assert false;
                }

                @Override public void delete(Object key) {
                    assert false;
                }
            });

            try {
                IgniteCache<Object, Object> cache = g.cache(null);

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

        IgniteCache<Object, Object> cache = G.ignite().cache(null);

        assert cache != null;

        Collection<Long> keys = new ArrayList<>(batchSize);

        for (long i = 0; i < keysCnt; i++) {
            keys.add(i);

            if (keys.size() == batchSize) {
                for (Long key : keys)
                    cache.localEvict(Collections.<Object>singleton(key));

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
                for (Long key : keys)
                    cache.localPromote(Collections.singleton(key));

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
     */
    @SuppressWarnings("unchecked")
    private static Ignite start(CacheStore<Long, String> store) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        TcpDiscoveryIpFinder finder = new TcpDiscoveryVmIpFinder(true);

        disco.setIpFinder(finder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setSwapEnabled(true);
        ccfg.setEvictSynchronized(false);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(EVICT_PLC_SIZE);

        ccfg.setEvictionPolicy(plc);

        if (store != null) {
            ccfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(store));
            ccfg.setReadThrough(true);
            ccfg.setWriteThrough(true);
            ccfg.setLoadPreviousValue(true);
        }

        FileSwapSpaceSpi swap = new FileSwapSpaceSpi();

//        swap.setConcurrencyLevel(16);
//        swap.setWriterThreadsCount(16);

//        swap.setLevelDbCacheSize(128 * 1024 * 1024);
//        swap.setLevelDbWriteBufferSize(128 * 1024 * 1024);
//        swap.setLevelDbBlockSize(1024 * 1024);
//        swap.setLevelDbParanoidChecks(false);
//        swap.setLevelDbVerifyChecksums(false);

        cfg.setSwapSpaceSpi(swap);

        ccfg.setCacheMode(CacheMode.LOCAL);

        cfg.setCacheConfiguration(ccfg);

        return G.start(cfg);
    }

}