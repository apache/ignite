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

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.LongPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.PrewarmingConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.internal.pagemem.PageIdUtils.PAGE_IDX_MASK;
import static org.apache.ignite.internal.pagemem.PageIdUtils.PAGE_IDX_SIZE;
import static org.apache.ignite.internal.pagemem.PageIdUtils.PART_ID_MASK;
import static org.apache.ignite.internal.pagemem.PageIdUtils.PART_ID_SIZE;

/**
 *
 */
public class LastLoadedPagesIdsStore implements LifecycleAware, FullPageIdSource {
    /** Prewarming page IDs dump directory name. */
    private static final String PREWARM_DUMP_DIR = "prewarm";

    /** Data region name. */
    private final String dataRegName;

    /** Prewarming configuration. */
    private final PrewarmingConfiguration prewarmCfg;

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private final IgniteLogger log;

    /** Dump worker. */
    private volatile DumpWorker dumpWorker;

    /** Page memory. */
    private volatile PageMemoryEx pageMem;

    /** Page IDs store. */
    private volatile PageIdsDumpStore dumpStore;

    /** Stopping flag. */
    private volatile boolean stopping;

    /** Loaded pages dump in progress flag. */
    private volatile boolean loadedPagesDumpInProgress; // FIXME


    /**
     * @param dataRegName Data region name.
     * @param prewarmCfg Prewarming configuration.
     */
    public LastLoadedPagesIdsStore(
        String dataRegName,
        PrewarmingConfiguration prewarmCfg,
        GridCacheSharedContext<?, ?> ctx) {
        this.dataRegName = dataRegName;
        this.prewarmCfg = prewarmCfg;
        this.ctx = ctx;
        this.log = ctx.logger(LastLoadedPagesIdsStore.class);
    }

    /**
     *
     */
    void initStore() throws IgniteException {
        IgnitePageStoreManager store = ctx.pageStore();

        assert store instanceof FilePageStoreManager : "Invalid page store manager was created: " + store;

        File dumpDir = Paths.get(((FilePageStoreManager)store).workDir().getAbsolutePath(), PREWARM_DUMP_DIR).toFile();

        if (!U.mkdirs(dumpDir))
            throw new IgniteException("Could not create directory for prewarming data: " + dumpDir);

        dumpStore = new FilePageIdsDumpStore(dumpDir, log);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (prewarmCfg.getRuntimeDumpDelay() > PrewarmingConfiguration.RUNTIME_DUMP_DISABLED) {
            dumpWorker = new DumpWorker();

            new IgniteThread(dumpWorker).start();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        stopping = true;

        DumpWorker dumpWorker = this.dumpWorker;

        if (dumpWorker != null) {
            dumpWorker.wakeUp();

            try {
                dumpWorker.join();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteException(e);
            }
        }
        else
            dumpLoadedPagesIds();
    }

    /** {@inheritDoc} */
    @Override public void forEach(FullPageIdConsumer consumer, BooleanSupplier breakCond) {
        // TODO check running
        dumpStore.forEach(consumer, breakCond);
    }

    /**
     * @param pageMem Page memory.
     */
    void pageMemory(PageMemoryEx pageMem) {
        this.pageMem = pageMem;
    }

    /**
     *
     */
    private void dumpLoadedPagesIds() {
        /*if (prewarmThread != null) { // FIXME
            if (onStopping)
                U.warn(log, "Attempt dump of loaded pages IDs on stopping while prewarming process is running!");

            return;
        }*/

        loadedPagesDumpInProgress = true;

        try {
            if (log.isInfoEnabled())
                log.info("Starting dump of loaded pages IDs of DataRegion [name=" + dataRegName + "]");

            final long startTs = U.currentTimeMillis();

            final ConcurrentMap<Long, Partition> partMap = new ConcurrentHashMap<>();

            final LongPredicate skipPageIdPred = (pageId) ->
                prewarmCfg.isIndexesOnly() &&
                PageIdUtils.partId(pageId) != PageIdAllocator.INDEX_PARTITION;

            pageMem.forEachAsync((grpId, pageId, touchTs) -> {
                if (skipPageIdPred.test(pageId))
                    return;

                long segmentKey = Partition.key(grpId, pageId);

                Partition part = partMap.computeIfAbsent(segmentKey, Partition::new);

                part.incCount();
            }).get();

            pageMem.forEachAsync((grpId, pageId, touchTs) -> {
                if (skipPageIdPred.test(pageId))
                    return;

                long segmentKey = Partition.key(grpId, pageId);

                Partition seg = partMap.computeIfPresent(segmentKey, (key, seg0) -> {
                    seg0.resetModifiedAndGet(); // FIXME

                    return seg0;
                });

                if (seg != null)
                    seg.addPageIdx(pageId, startTs - touchTs);
            }).get();

            Collection<Partition> parts = partMap.values();

            long pageIdsCnt = 0;

            for (Partition seg : parts) {
                pageIdsCnt += seg.pageIdxI;

                seg.cnt = 0;
            }

            if (pageIdsCnt == 0) {
                if (log.isInfoEnabled())
                    log.info("No loaded pages in DataRegion [name=" + dataRegName + "], no dump was created");

                return;
            }

            double hottestZoneRatio = prewarmCfg.getHottestZoneRatio();

            String dumpId = dumpStore.createDump();

            try {
                if (hottestZoneRatio > .0 && hottestZoneRatio < 1.0) {
                    parts.forEach(part -> Arrays.sort(part.pageIdxTsArr, 0, part.pageIdxI));

                    long pageIdsLeft = (long)Math.floor(pageIdsCnt * hottestZoneRatio);

                    Partition head = Collections.min(parts, Partition.byCoolingAtCnt);

                    while (pageIdsLeft > 0) {
                        Partition head0 = head;

                        Optional<Partition> nextOpt = parts.stream()
                            .filter(part -> part != head0 && part.hasNext())
                            .min(Partition.byCoolingAtCnt);

                        if (!nextOpt.isPresent()) {
                            head.addCount(Math.min((int)pageIdsLeft, head.pageIdxI - head.cnt));

                            pageIdsLeft = 0;
                        }
                        else {
                            Partition next = nextOpt.get();

                            while (head.cnt < head.pageIdxI &&
                                Partition.byCoolingAtCnt.compare(head, next) <= 0) {
                                head.incCount();

                                if (--pageIdsLeft == 0)
                                    break;
                            }

                            head = next;
                        }
                    }

                    dumpStore.save(parts.stream().filter(part -> part.cnt > 0)
                        .map(part -> part.toDumpStorePartition(part::headPageIndexes))
                        .collect(Collectors.toList()));
                }

                dumpStore.save(parts.stream().filter(part -> part.pageIdxI - part.cnt > 0)
                    .map(part -> part.toDumpStorePartition(part::tailPageIndexes))
                    .collect(Collectors.toList()));
            }
            finally {
                dumpStore.finishDump();

                long dumpTime = U.currentTimeMillis() - startTs;

                if (log.isInfoEnabled()) {
                    log.info("Dump of loaded pages IDs of DataRegion [name=" + dataRegName + "] finished in " +
                        dumpTime + " ms [dumpId=" + dumpId + ", pageIdsCount=" + pageIdsCnt + "]");
                }
            }
        }
        catch (IgniteCheckedException | IgniteException e) {
            U.warn(log, "Dump of loaded pages IDs for DataRegion [name=" + dataRegName + "] failed", e);
        }
        finally {
            loadedPagesDumpInProgress = false; // FIXME ?
        }
    }

    /**
     *
     */
    private static class Partition {
        /** Group ID key mask. */
        private static final long GRP_ID_MASK = ~(-1L << 32);

        /** Id count field updater. */
        private static final AtomicIntegerFieldUpdater<Partition> cntUpd = AtomicIntegerFieldUpdater.newUpdater(
            Partition.class, "cnt");

        /** Page index array pointer field updater. */
        private static final AtomicIntegerFieldUpdater<Partition> pageIdxIUpd = AtomicIntegerFieldUpdater.newUpdater(
            Partition.class, "pageIdxI");

        /** Comparator by page index at count. */
        private static final Comparator<Partition> byCoolingAtCnt = Comparator.comparingInt(o -> o.cooling(o.cnt));

        /** Key. */
        final long key;

        /** Id count. */
        volatile int cnt;

        /** Modified flag. */
        volatile boolean modified;

        /** Page index and timestamp array. */
        volatile long[] pageIdxTsArr;

        /** Page index array pointer. */
        volatile int pageIdxI;

        // TODO Collection of sub-segments if cnt was dramatically grown.

        /**
         * @param key Key.
         */
        Partition(long key) {
            this.key = key;
        }

        /**
         *
         */
        int id() {
            return (int)(key & PART_ID_MASK);
        }

        /**
         *
         */
        int cacheId() {
            return (int)(key >> PART_ID_SIZE);
        }

        /**
         *
         */
        void incCount() {
            modified = true;

            cntUpd.incrementAndGet(this);
        }

        /**
         *
         */
        void decCount() {
            modified = true;

            cntUpd.decrementAndGet(this);
        }

        /**
         * @param delta Delta.
         */
        void addCount(int delta) {
            modified = true;

            cntUpd.addAndGet(this, delta);
        }

        /**
         * @param pageId Page ID.
         * @param cooling Cooling.
         */
        void addPageIdx(long pageId, long cooling) {
            int ptr = pageIdxIUpd.getAndIncrement(this);

            if (ptr < pageIdxTsArr.length)
                pageIdxTsArr[ptr] = ((cooling / 1000) << PAGE_IDX_SIZE) | PageIdUtils.pageIndex(pageId);
        }

        /**
         * @param ptr {@link #pageIdxTsArr} pointer.
         */
        int cooling(int ptr) {
            return (int)(pageIdxTsArr[ptr] >>> PAGE_IDX_SIZE);
        }

        /**
         * Returns {@code null} if {@link #modified} is {@code false}, otherwise resets {@link #pageIdxI},
         * creates new {@link #pageIdxTsArr} and returns {@code this}.
         */
        Partition resetModifiedAndGet() {
            if (!modified)
                return null;

            modified = false;

            pageIdxI = 0;

            pageIdxTsArr = new long[cnt];

            return this;
        }

        /**
         *
         */
        int[] headPageIndexes() {
            return pageIndexes(0, cnt);
        }

        /**
         *
         */
        int[] tailPageIndexes() {
            return pageIndexes(cnt, pageIdxI);
        }

        /**
         * @param fromIdx From index.
         * @param toIdx To index.
         */
        int[] pageIndexes(int fromIdx, int toIdx) {
            long[] pageIdxTsArr = this.pageIdxTsArr;

            assert fromIdx >= 0
                && fromIdx <= toIdx
                && toIdx <= pageIdxTsArr.length;

            int[] pageIndexes = new int[toIdx - fromIdx];

            for (int i = 0; i < pageIndexes.length; i++)
                pageIndexes[i] = (int)(pageIdxTsArr[i + fromIdx] & PAGE_IDX_MASK);

            return pageIndexes;
        }

        /**
         *
         */
        boolean hasNext() {
            return cnt < pageIdxI;
        }

        /**
         *
         */
        PageIdsDumpStore.Partition toDumpStorePartition(Supplier<int[]> pageIdxSupplier) {
            int[] pageIndexes = pageIdxSupplier.get();

            Arrays.sort(pageIndexes);

            return new PageIdsDumpStore.Partition(id(), cacheId(), pageIndexes);
        }

        /**
         * @param grpId Group id.
         * @param pageId Page id.
         */
        static long key(long grpId, long pageId) {
            return (((grpId & GRP_ID_MASK) << PART_ID_SIZE)) + PageIdUtils.partId(pageId);
        }
    }

    /**
     *
     */
    private class DumpWorker extends GridWorker {
        /** */
        private static final String NAME_SUFFIX = "-loaded-pages-ids-dump-worker";

        /** */
        private final Object monitor = new Object();

        /**
         * Default constructor.
         */
        DumpWorker() {
            super(ctx.igniteInstanceName(), dataRegName + NAME_SUFFIX, LastLoadedPagesIdsStore.this.log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!stopping && !isCancelled()) {

                long timeout;
                long wakeTs = prewarmCfg.getRuntimeDumpDelay() + U.currentTimeMillis();

                synchronized (monitor) {
                    while (!stopping && (timeout = wakeTs - U.currentTimeMillis()) > 0)
                        monitor.wait(timeout);
                }

                dumpLoadedPagesIds();
            }
        }

        /**
         *
         */
        void wakeUp() {
            synchronized (monitor) {
                monitor.notify();
            }
        }
    }
}
