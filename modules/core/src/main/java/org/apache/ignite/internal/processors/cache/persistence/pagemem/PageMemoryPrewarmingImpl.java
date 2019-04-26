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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.PrewarmingConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.internal.stat.IoStatisticsType.CACHE_GROUP;

/**
 * Default {@link PageMemoryPrewarming} implementation.
 */
public class PageMemoryPrewarmingImpl implements PageMemoryPrewarming {
    /** Throttle time in nanoseconds. */
    private static final long THROTTLE_TIME_NANOS = 4_000;

    /** How many last seconds we will check for throttle. */
    private static final int THROTTLE_FIRST_CHECK_PERIOD = 3;

    /** How many last seconds we will check for throttle. */
    private static final int THROTTLE_CHECK_FREQUENCY = 1_000;

    /** Data region name. */
    private final String dataRegName;

    /** Prewarming configuration. */
    private final PrewarmingConfiguration prewarmCfg;

    /** Last loaded page IDs store. */
    private final LastLoadedPagesIdsStore lastLoadedPagesIdsStore;

    /** Custom prewarming page IDs supplier. */
    private final Supplier<Map<String, Map<Integer, Supplier<int[]>>>> customPageIdsSupplier;

    /** Data region metrics. */
    private final DataRegionMetrics dataRegMetrics;

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private final IgniteLogger log;

    /** Need throttling. */
    private final AtomicBoolean needThrottling = new AtomicBoolean();

    /** Throttling count. */
    private final AtomicLong throttlingCnt = new AtomicLong();

    /** Read rates last timestamp. */
    private final AtomicLong readRatesLastTs = new AtomicLong();

    /** Reads values. */
    private final long[] readsVals = new long[THROTTLE_FIRST_CHECK_PERIOD];

    /** Reads rates. */
    private final long[] readsRates = new long[THROTTLE_FIRST_CHECK_PERIOD + 1]; // + ceil for rates

    /** Page memory. */
    private volatile PageMemoryEx pageMem;

    /** Prewarming thread. */
    private volatile Thread prewarmThread;

    /** Stop prewarming flag. */
    private volatile boolean stopPrewarm;

    /** Stopping flag. */
    private volatile boolean stopping;

    /**
     * @param dataRegName Data region name.
     * @param prewarmCfg Prewarming configuration.
     * @param dataRegMetrics Data region metrics.
     * @param ctx Cache shared context.
     */
    public PageMemoryPrewarmingImpl(
        String dataRegName,
        PrewarmingConfiguration prewarmCfg,
        DataRegionMetrics dataRegMetrics,
        GridCacheSharedContext<?, ?> ctx) {

        this.dataRegName = dataRegName;

        assert prewarmCfg != null;

        this.prewarmCfg = prewarmCfg;
        this.dataRegMetrics = dataRegMetrics;

        assert ctx != null;

        this.ctx = ctx;
        this.log = ctx.logger(PageMemoryPrewarmingImpl.class);

        customPageIdsSupplier = prewarmCfg.getCustomPageIdsSupplier();

        lastLoadedPagesIdsStore = customPageIdsSupplier != null ? null :
            new LastLoadedPagesIdsStore(dataRegName, prewarmCfg, ctx);
    }

    /** {@inheritDoc} */
    @Override public void pageMemory(PageMemoryEx pageMem) {
        this.pageMem = pageMem;

        if (lastLoadedPagesIdsStore != null)
            lastLoadedPagesIdsStore.pageMemory(pageMem);
    }

    /** {@inheritDoc} */
    @Override public void onPageReplacementStarted() {
        stopPrewarm = true;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (lastLoadedPagesIdsStore != null)
            lastLoadedPagesIdsStore.initStore();

        if (prewarmCfg.isWaitPrewarmingOnStart()) {
            prewarmThread = Thread.currentThread();

            prewarm();
        }
        else {
            prewarmThread = new IgniteThread(
                ctx.igniteInstanceName(),
                threadNamePrefix(),
                this::prewarm);

            prewarmThread.setUncaughtExceptionHandler(ctx.kernalContext().uncaughtExceptionHandler());
            prewarmThread.setDaemon(true);
            prewarmThread.start();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        stopping = true;
        stopPrewarm = true;

        try {
            Thread prewarmThread = this.prewarmThread;

            if (prewarmThread != null)
                prewarmThread.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteException(e);
        }

        if (lastLoadedPagesIdsStore != null)
            lastLoadedPagesIdsStore.stop();
        else if (customPageIdsSupplier instanceof LifecycleAware)
            ((LifecycleAware)customPageIdsSupplier).stop();
    }

    /**
     * @return {@code true} if prewarming process should be stopped.
     */
    private boolean isStopPrewarm() {
        return stopPrewarm;
    }

    /**
     * @return Thread name prefix for worker threads.
     */
    private String threadNamePrefix() {
        return dataRegName + "-prewarm";
    }

    /**
     *
     */
    private void prewarm() {
        if (log.isInfoEnabled())
            log.info("Start prewarming of DataRegion [name=" + dataRegName + "]");

        initReadsRate();

        needThrottling.set(readsRates[readsRates.length - 1] == Long.MAX_VALUE);

        if (needThrottling.get() && log.isInfoEnabled())
            log.info("Detected need to throttle warming up.");

        long startTs = U.currentTimeMillis();

        readRatesLastTs.set(startTs);

        FullPageIdSource pageIdSrc = lastLoadedPagesIdsStore != null ? lastLoadedPagesIdsStore :
            this::forEachCustomPageIds;

        PageLoader pageLdr = new PageLoader();

        try {
            pageIdSrc.forEach(pageLdr, this::isStopPrewarm);

            IgniteInternalFuture<Void> finishFut = pageLdr.finishFuture();

            finishFut.get();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Prewarming of DataRegion [name=" + dataRegName + "] failed", e);
        }
        finally {
            long warmingUpTime = U.currentTimeMillis() - startTs;

            if (log.isInfoEnabled()) {
                log.info("Prewarming of DataRegion [name=" + dataRegName +
                    "] finished in " + warmingUpTime + " ms, pages prewarmed: " + pageLdr.loadedPagesCount());
            }

            prewarmThread = null;

            if (lastLoadedPagesIdsStore != null)
                lastLoadedPagesIdsStore.start();
            else if (customPageIdsSupplier instanceof LifecycleAware)
                ((LifecycleAware)customPageIdsSupplier).start();
        }
    }

    /**
     * @param consumer Full page ID consumer.
     * @param breakCond Break condition.
     */
    private void forEachCustomPageIds(FullPageIdConsumer consumer, BooleanSupplier breakCond) {
        if (customPageIdsSupplier == null)
            return;

        Map<String, Map<Integer, Supplier<int[]>>> pageIdsMap = customPageIdsSupplier.get();

        if (pageIdsMap == null || breakCond != null && breakCond.getAsBoolean())
            return;

        for (Map.Entry<String, Map<Integer, Supplier<int[]>>> entry : pageIdsMap.entrySet()) {
            String cacheName = entry.getKey();

            if (cacheName == null || cacheName.isEmpty()) {
                U.warn(log, "Invalid (null or empty) custom value of cache name encountered.");

                continue;
            }

            int cacheId = CU.cacheId(cacheName);

            for (Map.Entry<Integer, Supplier<int[]>> part : entry.getValue().entrySet()) {
                if (part.getKey() == null) {
                    U.warn(log, "Invalid (null) custom value of partition ID encountered for cache [name=" +
                        cacheName + "].");

                    continue;
                }

                int partId = part.getKey();

                if (partId > PageIdAllocator.MAX_PARTITION_ID && partId != PageIdAllocator.INDEX_PARTITION) {
                    U.warn(log, "Invalid (" + U.hexInt(partId) +
                        ") custom value of partition ID encountered for cache [name=" +
                        cacheName + "].");
                }

                if (prewarmCfg.isIndexesOnly() && partId != PageIdAllocator.INDEX_PARTITION) {
                    U.warn(log, "Invalid (non-index: " + U.hexInt(partId) +
                        ") custom value of partition ID encountered for cache [name=" + cacheName +
                        "] while prewarming of indexes only is set.");

                    continue;
                }

                Supplier<int[]> pageIdxSupplier = part.getValue();

                if (pageIdxSupplier == null) {
                    U.warn(log, "Invalid (null) page index supplier encountered for partition [cacheName=" +
                        cacheName + ", partId=" + partId + "].");

                    continue;
                }

                if (pageIdxSupplier == PrewarmingConfiguration.WHOLE_PARTITION) {
                    // TODO
                    /*try {
                        if (ctx.cache().cache(grpName).localPreloadPartition(partId)) {
                            IgnitePageStoreManager pageStoreMgr = ctx.pageStore();

                            if (pageStoreMgr != null)
                                pagesPrewarmed.addAndGet(pageStoreMgr.pages(CU.cacheId(grpName), partId));
                        }
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to preload partition [id=" + partId +
                            "] of cache group [name=" + grpName + "]", e);
                    }*/
                }
                else {
                    int[] pageIdxArr = pageIdxSupplier.get();

                    if (pageIdxArr == null || pageIdxArr.length == 0)
                        continue;

                    for (int pageIdx : pageIdxArr) {
                        consumer.accept(cacheId, PageIdUtils.pageId(partId, pageIdx));

                        if (breakCond != null && breakCond.getAsBoolean())
                            return;
                    }
                }
            }
        }
    }

    /**
     * Get first IO rates and set ceil for them into last element of rates array.
     */
    private void initReadsRate() {
        readsVals[readsVals.length - 1] = ctx.kernalContext().ioStats().totalPhysicalReads(CACHE_GROUP);

        if (readsVals[readsVals.length - 1] == Long.MAX_VALUE) {
            Arrays.fill(readsVals, Long.MAX_VALUE);
            Arrays.fill(readsRates, Long.MAX_VALUE);

            return;
        }

        for (int i = 0; i < THROTTLE_FIRST_CHECK_PERIOD; i++) {
            LockSupport.parkNanos(1_000_000_000L);

            refreshRates();
        }

        readsRates[readsRates.length - 1] = readsRates[readsRates.length - 2];
    }

    /**
     * Set new IO read rate for last element in rates array.
     *
     * @return New IO read rate.
     */
    private long refreshRates() {
        for (int i = 0; i < readsVals.length - 1; i++) {
            readsVals[i] = readsVals[i + 1];
            readsRates[i] = readsRates[i + 1];
        }

        readsVals[readsVals.length - 1] = ctx.kernalContext().ioStats().totalPhysicalReads(CACHE_GROUP);
        readsRates[readsVals.length - 1] = calculateRate(readsVals);

        return readsRates[readsVals.length - 1];
    }

    /**
     * @param reads IO reads.
     * @return IO rate for given reads.
     */
    private long calculateRate(long[] reads) {
        long curRate = 0;

        for (int i = 1; i < reads.length; i++) {
            long delta = reads[i] - reads[0];

            if (curRate + delta / reads.length < 0)
                return Long.MAX_VALUE;

            curRate += delta / reads.length;
        }

        return curRate;
    }

    /**
     * Check that we need to throttle warmUp.
     *
     * @return {@code True} if warmUp needs throttling.
     */
    private boolean needThrottling() {
        if (readsVals[readsVals.length - 1] == Long.MAX_VALUE)
            return true;

        if (readsVals[0] == 0)
            return false;

        long highBorder = readsRates[readsRates.length - 1] + (long) (readsRates[readsRates.length - 1]
            * prewarmCfg.getThrottleAccuracy());
        long lowBorder = readsRates[readsRates.length - 1] - (long) (readsRates[readsRates.length - 1]
            * prewarmCfg.getThrottleAccuracy());

        if (needThrottling.get()) {
            if (allLower(readsRates, lowBorder))
                return false;

            if (allHigher(readsRates, highBorder)) {
                readsRates[readsRates.length - 1] = readsRates[readsRates.length - 2]; // inc ceil

                return false;
            }

            return true;
        }

        if (allHigher(readsRates, highBorder)) {
            readsRates[readsRates.length - 1] = readsRates[readsRates.length - 2]; // inc ceil

            return false;
        }

        if (allHigher(readsRates, lowBorder)) {
            if (log.isInfoEnabled())
                log.info("Detected need to throttle warming up.");

            return true;
        }

        return false;
    }

    /**
     * @param rates IO read rates.
     * @param border Top border.
     * @return {@code True} if all rates are higher than given value.
     */
    private boolean allHigher(long[] rates, long border) {
        for (int i = 0; i < rates.length - 1; i++) {
            if (rates[i] <= border)
                return false;
        }

        return true;
    }

    /**
     * @param rates IO read rates.
     * @param border Top border.
     * @return {@code True} if all rates are lower than given value.
     */
    private boolean allLower(long[] rates, long border) {
        for (int i = 0; i < rates.length - 1; i++) {
            if (rates[i] >= border)
                return false;
        }

        return true;
    }

    /** */
    private class PageLoader implements FullPageIdConsumer {
        /** Striped thread pool executor for multithreaded page loading. */
        private final IgniteStripedThreadPoolExecutor stripedExecutor;

        /** Stripe selector. */
        private final StripeSelector stripeSelector;

        /** Loaded pages count. */
        private final AtomicLong loadedPagesCnt = new AtomicLong();

        /** Finish future reference. */
        private final AtomicReference<IgniteInternalFuture<Void>> finishFutRef = new AtomicReference<>();

        /**
         * Default constructor.
         */
        PageLoader() {
            int pageLoadThreads = prewarmCfg.getPageLoadThreads();

            if (pageLoadThreads > 1) {
                stripedExecutor = new IgniteStripedThreadPoolExecutor(
                    pageLoadThreads,
                    ctx.igniteInstanceName(),
                    "prewarm",
                    ctx.kernalContext().uncaughtExceptionHandler()
                );

                stripeSelector = pageMem instanceof PageMemoryImpl ?
                    new SegmentBasedStripeSelector((PageMemoryImpl)pageMem, pageLoadThreads) :
                    new CyclicStripeSelector(pageLoadThreads);
            }
            else {
                stripedExecutor = null;
                stripeSelector = null;

                finishFutRef.set(new GridFinishedFuture<>());
            }
        }

        /** {@inheritDoc} */
        @Override public void accept(int grpId, long pageId) {
            if (prewarmCfg.isIndexesOnly() && PageIdUtils.partId(pageId) != PageIdAllocator.INDEX_PARTITION)
                return;

            if (stripedExecutor != null) {
                stripedExecutor.execute(
                    () -> loadPage(grpId, pageId),
                    stripeSelector.selectStripe(grpId, pageId));
            }
            else
                loadPage(grpId, pageId);
        }

        /**
         * Loads page with given group ID and page ID into memory.
         *
         * @param grpId Page group ID.
         * @param pageId Page ID.
         */
        public void loadPage(int grpId, long pageId) {
            try {
                long page = pageMem.acquirePage(grpId, pageId);

                pageMem.releasePage(grpId, pageId, page);

                loadedPagesCnt.incrementAndGet();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to load page [grpId=" + grpId + ", pageId=" + pageId + ']', e);
            }

            incThrottleCounter();

            if (needThrottling.get())
                LockSupport.parkNanos(THROTTLE_TIME_NANOS);
        }

        /**
         * Should be called after {@code this} instance used as {@link FullPageIdConsumer}.
         *
         * @return Page loading finish future.
         */
        public IgniteInternalFuture<Void> finishFuture() {
            IgniteInternalFuture<Void> fut = finishFutRef.get();

            if (fut != null)
                return fut;

            int concurrencyLevel = stripedExecutor.concurrencyLevel();

            CountDownFuture fut0 = new CountDownFuture(concurrencyLevel) {
                @Override protected void afterDone() {
                    stripedExecutor.shutdownNow();
                }
            };

            if (!finishFutRef.compareAndSet(null, fut0))
                return finishFutRef.get();

            Runnable finish = fut0::onDone;

            for (int i = 0; i < concurrencyLevel; i++)
                stripedExecutor.execute(finish, i);

            return fut0;
        }

        /**
         *
         */
        public long loadedPagesCount() {
            return loadedPagesCnt.get();
        }

        /**
         * Increase throttle counter and check throttling need.
         */
        private void incThrottleCounter() {
            long cnt = throttlingCnt.addAndGet(1L);

            if (cnt >= THROTTLE_CHECK_FREQUENCY) {
                synchronized (throttlingCnt) {
                    cnt = throttlingCnt.get();

                    if (cnt >= THROTTLE_CHECK_FREQUENCY) {
                        long newTs = U.currentTimeMillis();

                        if (newTs - readRatesLastTs.get() >= 1) {
                            refreshRates();

                            readRatesLastTs.set(newTs);

                            needThrottling.set(needThrottling());
                        }
                    }

                    throttlingCnt.set(-THROTTLE_CHECK_FREQUENCY);
                }
            }
        }
    }

    /**
     * Functional interface for selecting stripe index for specified cache group ID and page ID.
     */
    private interface StripeSelector {
        /**
         * @param grpId Cache group ID.
         * @param pageId Page ID.
         */
        public int selectStripe(int grpId, long pageId);
    }

    /**
     * Selects next stripe index using cyclic counter.
     */
    private static class CyclicStripeSelector implements StripeSelector {
        /** Counter. */
        private final AtomicInteger cntr = new AtomicInteger();

        /** Stripe count. */
        private final int stripeCnt;

        /**
         * @param stripeCnt Stripe count.
         */
        private CyclicStripeSelector(int stripeCnt) {
            assert stripeCnt > 1;

            this.stripeCnt = stripeCnt;
        }

        /** {@inheritDoc} */
        @Override public int selectStripe(int grpId, long pageId) {
            while (true) {
                int idx = cntr.get();

                int nextIdx = idx + 1;

                if (nextIdx == stripeCnt)
                    nextIdx = 0;

                if (cntr.compareAndSet(idx, nextIdx))
                    return idx;
            }
        }
    }

    /**
     * Selects stripe index based on segment index provided by {@link PageMemoryImpl}.
     */
    private static class SegmentBasedStripeSelector implements StripeSelector {
        /** Page memory. */
        private final PageMemoryImpl pageMemory;

        /** Stripe count. */
        private final int stripeCnt;

        /**
         * @param pageMemory Page memory.
         * @param stripeCnt Stripe count.
         */
        private SegmentBasedStripeSelector(PageMemoryImpl pageMemory, int stripeCnt) {
            this.pageMemory = pageMemory;
            this.stripeCnt = stripeCnt;
        }

        /** {@inheritDoc} */
        @Override public int selectStripe(int grpId, long pageId) {
            int segIdx = pageMemory.segmentIndex(grpId, pageId);

            return segIdx % stripeCnt;
        }
    }
}
