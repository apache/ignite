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
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.regex.Pattern;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.internal.pagemem.PageIdUtils.PART_ID_SIZE;

/**
 * Default {@link PageMemoryWarmingUp} implementation.
 */
public class PageMemoryWarmingUpImpl implements PageMemoryWarmingUp, LoadedPagesTracker {
    /** Warm up directory name. */
    private static final String WARM_UP_DIR = "warm";

    /** Data region configuration. */
    private final DataRegionConfiguration dataRegCfg;

    /** Data region metrics. */
    private final DataRegionMetrics dataRegMetrics;

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ConcurrentMap<Long, Segment> segments = new ConcurrentHashMap<>();

    /** Page memory. */
    private volatile PageMemoryEx pageMem;

    /** Warm up directory. */
    private File warmUpDir;

    /** Warm up thread. */
    private volatile Thread warmUpThread;

    /** Stop warming up flag. */
    private volatile boolean stopWarmingUp;

    /** Dump worker. */
    private volatile LoadedPagesIdsDumpWorker dumpWorker;

    /** Loaded pages dump in progress flag. */
    private volatile boolean loadedPagesDumpInProgress;

    /** Stopping flag. */
    private volatile boolean stopping;

    /**
     * @param dataRegCfg Data region configuration.
     * @param dataRegMetrics Data region metrics.
     * @param ctx Cache shared context.
     */
    public PageMemoryWarmingUpImpl(
        DataRegionConfiguration dataRegCfg,
        DataRegionMetrics dataRegMetrics,
        GridCacheSharedContext<?, ?> ctx) {
        assert dataRegCfg != null;

        this.dataRegCfg = dataRegCfg;
        this.dataRegMetrics = dataRegMetrics;

        assert ctx != null;

        this.ctx = ctx;
        this.log = ctx.logger(PageMemoryWarmingUpImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void pageMemory(PageMemoryEx pageMem) {
        this.pageMem = pageMem;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        initDir();

        if (dataRegCfg.isWaitWarmingUpOnStart()) {
            warmUpThread = Thread.currentThread();

            warmUp();
        }
        else {
            warmUpThread = new IgniteThread(
                ctx.igniteInstanceName(),
                dataRegCfg.getName() + "-warm-up",
                this::warmUp);

            warmUpThread.setDaemon(true);
            warmUpThread.start();
        }

        if (dataRegCfg.getWarmingUpRuntimeDumpDelay() >= 0) {
            dumpWorker = new LoadedPagesIdsDumpWorker();

            new IgniteThread(dumpWorker).start();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        stopping = true;

        try {
            Thread warmUpThread = this.warmUpThread;

            if (warmUpThread != null)
                warmUpThread.join();

            if (dumpWorker != null) {
                if (!loadedPagesDumpInProgress)
                    dumpWorker.cancel();

                dumpWorker.join();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteException(e);
        }

        dumpLoadedPagesIds(true);
    }

    /** {@inheritDoc} */
    @Override public void onPageLoad(int grpId, long pageId) {
        if (dataRegCfg.isWarmingUpIndexesOnly() &&
            PageIdUtils.partId(pageId) != PageIdAllocator.INDEX_PARTITION)
            return;

        getSegment(Segment.key(grpId, pageId)).incCount();
    }

    /** {@inheritDoc} */
    @Override public void onPageUnload(int grpId, long pageId) {
        if (dataRegCfg.isWarmingUpIndexesOnly() &&
            PageIdUtils.partId(pageId) != PageIdAllocator.INDEX_PARTITION)
            return;

        getSegment(Segment.key(grpId, pageId)).decCount();
    }

    /** {@inheritDoc} */
    @Override public void onPageEvicted(int grpId, long pageId) {
        stopWarmingUp = true;

        onPageUnload(grpId, pageId);
    }

    /**
     *
     */
    private void initDir() throws IgniteException {
        IgnitePageStoreManager store = ctx.pageStore();

        assert store instanceof FilePageStoreManager : "Invalid page store manager was created: " + store;

        warmUpDir = Paths.get(((FilePageStoreManager)store).workDir().getAbsolutePath(), WARM_UP_DIR).toFile();

        if (!U.mkdirs(warmUpDir))
            throw new IgniteException("Could not create directory for warming-up data: " + warmUpDir);
    }

    /**
     *
     */
    private void warmUp() {
        try {
            if (log.isInfoEnabled())
                log.info("Starting warming-up of DataRegion[name=" + dataRegCfg.getName() + "]");

            File[] segFiles = warmUpDir.listFiles(Segment.FILE_FILTER);

            if (segFiles == null) {
                if (log.isInfoEnabled())
                    log.info("Saved warming-up files not found!");

                return;
            }

            if (log.isInfoEnabled())
                log.info("Saved warming-up files found: " + segFiles.length);

            int pagesWarmed = 0;

            long startTs = U.currentTimeMillis();

            // TODO multithreaded processing of segment files
            for (File segFile : segFiles) {
                boolean del = true;

                try {
                    if (stopping || stopWarmingUp)
                        continue;

                    int partId = Integer.parseInt(segFile.getName().substring(
                        Segment.GRP_ID_PREFIX_LENGTH,
                        Segment.FILE_NAME_LENGTH), 16);

                    if (dataRegCfg.isWarmingUpIndexesOnly() && partId != PageIdAllocator.INDEX_PARTITION)
                        continue;

                    int grpId = (int)Long.parseLong(segFile.getName().substring(0, Segment.GRP_ID_PREFIX_LENGTH), 16);

                    int[] pageIdxArr = loadPageIndexes(segFile);

                    Arrays.sort(pageIdxArr);

                    del = false;

                    for (int pageIdx : pageIdxArr) {
                        if (stopping || stopWarmingUp) {
                            del = true;

                            break;
                        }

                        long pageId = PageIdUtils.pageId(partId, pageIdx);

                        try {
                            long page = pageMem.acquirePage(grpId, pageId);

                            pageMem.releasePage(grpId, pageId, page);

                            pagesWarmed++;
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to acquire page [grpId=" + grpId + ", pageId=" + pageId + "]", e);

                            del = true;
                        }

                        // TODO use dataRegMetrics for heuristic throttling
                    }
                }
                catch (IOException | NumberFormatException e) {
                    U.error(log, "Failed to read warming-up file: " + segFile.getName(), e);
                }
                finally {
                    if (del && !segFile.delete())
                        U.warn(log, "Failed to delete warming-up file: " + segFile.getName());
                }
            }

            long warmingUpTime = U.currentTimeMillis() - startTs;

            if (log.isInfoEnabled()) {
                log.info("Warming-up of DataRegion[name=" + dataRegCfg.getName() + "] finished in " +
                    warmingUpTime + " ms, pages warmed: " + pagesWarmed);
            }
        }
        finally {
            warmUpThread = null;
        }
    }

    /**
     * @param onStopping On stopping.
     */
    private void dumpLoadedPagesIds(boolean onStopping) {
        if (warmUpThread != null) {
            if (onStopping)
                U.warn(log, "Attempt dump of loaded pages IDs on stopping while warming-up process is running!");

            return;
        }

        loadedPagesDumpInProgress = true;

        try {
            if (!onStopping && stopping)
                return;

            if (log.isInfoEnabled())
                log.info("Starting dump of loaded pages IDs of DataRegion[name=" + dataRegCfg.getName() + "]");

            final ConcurrentMap<Long, Segment> updated = new ConcurrentHashMap<>();

            long startTs = U.currentTimeMillis();

            pageMem.forEachAsync((fullId, val) -> {
                if (dataRegCfg.isWarmingUpIndexesOnly() &&
                    PageIdUtils.partId(fullId.pageId()) != PageIdAllocator.INDEX_PARTITION)
                    return;

                Segment seg = !onStopping && stopping ?
                    updated.get(
                        Segment.key(fullId.groupId(), fullId.pageId())) :
                    updated.computeIfAbsent(
                        Segment.key(fullId.groupId(), fullId.pageId()),
                        key -> getSegment(key).resetModifiedAndGet());

                if (seg != null)
                    seg.addPageIdx(fullId.pageId());
            }).get();

            int segUpdated = 0;

            // TODO multithreaded processing of updated segments
            for (Segment seg : updated.values()) {
                if (!onStopping && stopping && seg.modified)
                    continue;

                try {
                    updateSegment(seg);

                    segUpdated++;
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }
            }

            long dumpTime = U.currentTimeMillis() - startTs;

            if (log.isInfoEnabled()) {
                log.info("Dump of loaded pages IDs of DataRegion[name=" + dataRegCfg.getName() + "] finished in " +
                    dumpTime + " ms, segments updated: " + segUpdated);
            }
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Dump of loaded pages IDs for DataRegion[name=" + dataRegCfg.getName() + "] failed", e);
        }
        finally {
            loadedPagesDumpInProgress = false;
        }
    }

    /**
     * @param key Segment key.
     */
    private Segment getSegment(long key) {
        return segments.computeIfAbsent(key, Segment::new);
    }

    /**
     * @param segFile Segment file.
     */
    private int[] loadPageIndexes(File segFile) throws IOException {
        try (FileIO io = new RandomAccessFileIO(segFile, StandardOpenOption.READ)) {
            int[] pageIdxArr = new int[(int)(io.size() / Integer.BYTES)];

            byte[] intBytes = new byte[Integer.BYTES];

            for (int i = 0; i < pageIdxArr.length; i++) {
                io.read(intBytes, 0, intBytes.length);

                pageIdxArr[i] = U.bytesToInt(intBytes, 0);
            }

            return pageIdxArr;
        }
    }

    /**
     * @param seg Segment.
     */
    private void updateSegment(Segment seg) throws IOException {
        int[] pageIdxArr = seg.pageIdxArr;

        seg.pageIdxArr = null;

        File segFile = new File(warmUpDir, seg.fileName());

        if (pageIdxArr.length == 0) {
            if (!segFile.delete())
                U.warn(log, "Failed to delete warming-up file: " + segFile.getName());

            return;
        }

        try (FileIO io = new RandomAccessFileIO(segFile,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

            byte[] chunk = new byte[Integer.BYTES * 1024];

            int chunkPtr = 0;

            for (int pageIdx : pageIdxArr) {
                chunkPtr = U.intToBytes(pageIdx, chunk, chunkPtr);

                if (chunkPtr == chunk.length) {
                    io.write(chunk, 0, chunkPtr);
                    io.force();

                    chunkPtr = 0;
                }
            }

            if (chunkPtr > 0) {
                io.write(chunk, 0, chunkPtr);
                io.force();
            }
        }
    }

    /**
     *
     */
    private static class Segment {
        /** File extension. */
        private static final String FILE_EXT = ".seg";

        /** File name length. */
        private static final int FILE_NAME_LENGTH = 12;

        /** Group ID prefix length. */
        private static final int GRP_ID_PREFIX_LENGTH = 8;

        /** Group ID key mask. */
        private static final long GRP_ID_MASK = ~(-1L << 32);

        /** File name pattern. */
        private static final Pattern FILE_NAME_PATTERN = Pattern.compile("[0-9A-Fa-f]{" + FILE_NAME_LENGTH + "}\\" + FILE_EXT);

        /** File filter. */
        private static final FileFilter FILE_FILTER = file -> !file.isDirectory() && FILE_NAME_PATTERN.matcher(file.getName()).matches();

        /** Id count field updater. */
        private static final AtomicIntegerFieldUpdater<Segment> idCntUpd = AtomicIntegerFieldUpdater.newUpdater(
            Segment.class, "idCnt");

        /** Page index array pointer field updater. */
        private static final AtomicIntegerFieldUpdater<Segment> pageIdxIUpd = AtomicIntegerFieldUpdater.newUpdater(
            Segment.class, "pageIdxI");

        /** Key. */
        final long key;

        /** Id count. */
        volatile int idCnt;

        /** Modified flag. */
        volatile boolean modified;

        /** Page index array. */
        volatile int[] pageIdxArr;

        /** Page index array pointer. */
        volatile int pageIdxI;

        // TODO Collection of sub-segments if idCnt was dramatically grown.

        /**
         * @param key Key.
         */
        Segment(long key) {
            this.key = key;
        }

        /**
         *
         */
        String fileName() {
            SB b = new SB();

            String keyHex = Long.toHexString(key);

            for (int i = keyHex.length(); i < FILE_NAME_LENGTH; i++)
                b.a('0');

            return b.a(keyHex).a(FILE_EXT).toString();
        }

        /**
         *
         */
        void incCount() {
            modified = true;

            idCntUpd.incrementAndGet(this);
        }

        /**
         *
         */
        void decCount() {
            modified = true;

            idCntUpd.decrementAndGet(this);
        }

        /**
         * @param pageId Page id.
         */
        void addPageIdx(long pageId) {
            int ptr = pageIdxIUpd.getAndIncrement(this);

            if (ptr < pageIdxArr.length)
                pageIdxArr[ptr] = PageIdUtils.pageIndex(pageId);
        }

        /**
         * Returns {@code null} if {@link #modified} is {@code false}, otherwise resets {@link #pageIdxI},
         * creates new {@link #pageIdxArr} and returns {@code this}.
         */
        Segment resetModifiedAndGet() {
            if (!modified)
                return null;

            modified = false;

            pageIdxI = 0;

            pageIdxArr = new int[idCnt];

            return this;
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
    private class LoadedPagesIdsDumpWorker extends GridWorker {
        /** */
        private static final String NAME_SUFFIX = "-loaded-pages-ids-dump-worker";

        /**
         * Default constructor.
         */
        LoadedPagesIdsDumpWorker() {
            super(ctx.igniteInstanceName(), dataRegCfg.getName() + NAME_SUFFIX, PageMemoryWarmingUpImpl.this.log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!stopping && !isCancelled()) {
                Thread.sleep(dataRegCfg.getWarmingUpRuntimeDumpDelay());

                if (stopping)
                    break;

                dumpLoadedPagesIds(false);
            }
        }
    }
}
