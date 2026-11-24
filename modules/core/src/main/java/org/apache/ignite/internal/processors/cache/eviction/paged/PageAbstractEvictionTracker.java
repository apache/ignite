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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.TestOnly;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PAGE_EVICTION_NO_PROGRESS_TIMEOUT_MILLIS;
import static org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl.SEG_CNT;

/**
 *
 */
public abstract class PageAbstractEvictionTracker implements PageEvictionTracker {
    /** This number of least significant bits is dropped from timestamp. */
    private static final int COMPACT_TS_SHIFT = 8; // Enough if grid works for less than 17 years.

    /** Millis in day. */
    private static final int DAY = 24 * 60 * 60 * 1000;

    /** Initial backoff (nanoseconds) between consecutive no-progress eviction attempts. */
    private static final long EVICTION_BACKOFF_START_NANOS = 50_000L;

    /** Upper bound (nanoseconds) for the backoff between no-progress eviction attempts. */
    private static final long EVICTION_BACKOFF_MAX_NANOS = 1_000_000L;

    /**
     * Default maximum time (milliseconds) the size-aware eviction guard is willing to wait without progress before
     * giving up (see {@link #reserve(long, boolean)}). Configurable via
     * {@link IgniteSystemProperties#IGNITE_PAGE_EVICTION_NO_PROGRESS_TIMEOUT_MILLIS}.
     */
    public static final long DFLT_EVICTION_NO_PROGRESS_TIMEOUT_MILLIS = 5_000L;

    /** Page memory. */
    protected final PageMemoryNoStoreImpl pageMem;

    /** Tracking array size. */
    protected final int trackingSize;

    /** Logger. */
    private final IgniteLogger log;

    /** Base compact timestamp. */
    private final long baseCompactTs;

    /** Shared context. */
    private final GridCacheSharedContext sharedCtx;

    /** Data region configuration. */
    private final DataRegionConfiguration regCfg;

    /** Data region metrics. */
    private final DataRegionMetricsImpl memMetrics;

    /** Free list of the data region. It is created after the tracker, so it is resolved on {@link #start()}. */
    private AbstractFreeList<?> freeList;

    /**
     * Maximum time (milliseconds) the size-aware eviction guard is willing to wait without progress before giving up.
     * This is a liveness guard: a {@code evictDataPage()} call can evict nothing (for example when the sampled rows
     * are not actually reclaimable). Progress is a data page freed by the caller.
     * An eviction that is merely slow keeps resetting the timer; a stall that evicts nothing for the whole window
     * gives up instead of spinning.
     */
    private final long noProgressTimeoutMillis = IgniteSystemProperties.getLong(
        IGNITE_PAGE_EVICTION_NO_PROGRESS_TIMEOUT_MILLIS, DFLT_EVICTION_NO_PROGRESS_TIMEOUT_MILLIS);

    /**
     * Pages reserved by in-flight inserts (see {@link #reserve(long, boolean)}): only the rows' own pages, the empty pages pool
     * is a region-wide margin, half of which absorbs them. A reservation is released as a whole once its row (or batch of rows) is written.
     */
    private final AtomicLong reservedPages = new AtomicLong();

    /**
     * Whether the eviction is stalled: set by a best-effort reservation that gave up because the eviction made no
     * progress within {@link #noProgressTimeoutMillis}, cleared by a reservation that frees a data page.
     * While set, best-effort reservations (including the already waiting ones) give up after an
     * eviction attempt without progress, so a series of inserts does not wait for the timeout one by one.
     */
    private volatile boolean stalled;

    /**
     * @param pageMem Page memory.
     * @param plcCfg Data region configuration.
     * @param memMetrics Data region metrics.
     * @param sharedCtx Shared context.
     */
    PageAbstractEvictionTracker(
        PageMemoryNoStoreImpl pageMem,
        DataRegionConfiguration plcCfg,
        DataRegionMetricsImpl memMetrics,
        GridCacheSharedContext sharedCtx
    ) {
        this.pageMem = pageMem;

        this.sharedCtx = sharedCtx;

        regCfg = plcCfg;

        this.memMetrics = memMetrics;

        log = sharedCtx.logger(getClass());

        trackingSize = pageMem.totalPages();

        baseCompactTs = (U.currentTimeMillis() - DAY) >> COMPACT_TS_SHIFT;
        // We subtract day to avoid fail in case of daylight shift or timezone change.
    }

    /** {@inheritDoc} */
    @Override public void start() {
        freeList = (AbstractFreeList<?>)sharedCtx.database().freeList(regCfg.getName());
    }

    /**
     * @param pageIdx Page index.
     * @return true if all data rows of the page have been evicted
     * @throws IgniteCheckedException If failed.
     */
    final boolean evictDataPage(int pageIdx) throws IgniteCheckedException {
        long fakePageId = PageIdUtils.pageId(0, (byte)0, pageIdx);

        long page = pageMem.acquirePage(0, fakePageId);

        List<CacheDataRowAdapter> rowsToEvict;

        try {
            long pageAddr = pageMem.readLockForce(0, fakePageId, page);

            try {
                if (PageIO.getType(pageAddr) != PageIO.T_DATA)
                    return false; // Can't evict: page has been recycled into non-data page.

                DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

                long realPageId = PageIO.getPageId(pageAddr);

                if (!checkTouch(realPageId))
                    return false; // Can't evict: another thread concurrently invoked forgetPage()

                rowsToEvict = io.forAllItems(pageAddr, new DataPageIO.CC<CacheDataRowAdapter>() {
                    @Override public CacheDataRowAdapter apply(long link) throws IgniteCheckedException {
                        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

                        row.initFromLink(null, sharedCtx, pageMem, CacheDataRowAdapter.RowData.KEY_ONLY, false);

                        assert row.cacheId() != 0 : "Cache ID should be stored in rows of evictable cache";

                        return row;
                    }
                });
            }
            finally {
                pageMem.readUnlock(0, fakePageId, page);
            }
        }
        finally {
            pageMem.releasePage(0, fakePageId, page);
        }

        boolean evictionDone = !rowsToEvict.isEmpty();

        for (CacheDataRowAdapter dataRow : rowsToEvict) {
            GridCacheContext<?, ?> cacheCtx = sharedCtx.cacheContext(dataRow.cacheId());

            if (!cacheCtx.userCache()) {
                evictionDone = false;

                continue;
            }

            GridCacheEntryEx entryEx = cacheCtx.isNear() ? cacheCtx.near().dht().entryEx(dataRow.key()) :
                cacheCtx.cache().entryEx(dataRow.key());

            evictionDone &= entryEx.evictInternal(GridCacheVersionManager.EVICT_VER, null, true);
        }

        return evictionDone;
    }

    /**
     * @param pageId Page ID.
     * @return true if page was touched at least once.
     */
    protected abstract boolean checkTouch(long pageId);

    /**
     * @param epochMilli Time millis.
     * @return Compact timestamp. Comparable and fits in 4 bytes.
     */
    final long compactTimestamp(long epochMilli) {
        return (epochMilli >> COMPACT_TS_SHIFT) - baseCompactTs;
    }

    /**
     * Resolves position in tracking array by page index.
     *
     * @param pageIdx Page index.
     * @return Position of page in tracking array.
     */
    int trackingIdx(int pageIdx) {
        return pageMem.pageSequenceNumber(pageIdx);
    }

    /**
     * Reverse of {@link #trackingIdx(int)}.
     *
     * @param trackingIdx Tracking index.
     * @return Page index.
     */
    int pageIdx(int trackingIdx) {
        return pageMem.pageIndex(trackingIdx);
    }

    /** {@inheritDoc} */
    @Override public void trackFragmentPage(long pageId, long prevPageId, boolean isHeadPage) {
        if (isHeadPage)
            initPage(pageId);

        // Do nothing if called for tail page.
        if (prevPageId == 0)
            return;

        if (isHeadPage) {
            // Store link to head fragment page in tail fragment page.
            linkFragmentPages(tailPageTrackingIdx(prevPageId), trackingIdx(PageIdUtils.pageIndex(pageId)));
        }
        else {
            // Store link to tail fragment page in each fragment page.
            linkFragmentPages(trackingIdx(PageIdUtils.pageIndex(pageId)), tailPageTrackingIdx(prevPageId));
        }
    }

    /** */
    protected abstract void initPage(long pageId);

    /**
     * Determine tail page tracking index given page id of previously written fragment.
     *
     * @param prevPageId Page id of previously written fragment.
     * @return tail page tracking index.
     */
    private int tailPageTrackingIdx(long prevPageId) {
        int prevPageTrackingIdx = trackingIdx(PageIdUtils.pageIndex(prevPageId));

        int link = getFragmentLink(prevPageTrackingIdx);

        if (link == 0) {
            // The previous page is just the tail one.
            return prevPageTrackingIdx;
        }
        else
            return link;
    }

    /**
     * Link two pages containing fragments of row.
     *
     * @param pageTrackingIdx     Page tracking index.
     * @param nextPageTrackingIdx Tracking index of page to link to.
     */
    protected abstract void linkFragmentPages(int pageTrackingIdx, int nextPageTrackingIdx);

    /**
     * Get tracking index of page linked to this one.
     *
     * @param pageTrackingIdx Page tracking index.
     * @return Tracking index of linked page, or {@code 0} if this page is not linked to any.
     */
    protected abstract int getFragmentLink(int pageTrackingIdx);

    /** {@inheritDoc} */
    @Override public boolean reserveEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long reserve(long rowSize, boolean bestEffort) throws IgniteOutOfMemoryException, IgniteCheckedException {
        // Nothing to insert (e.g. an empty batch): no eviction is needed.
        if (rowSize <= 0)
            return 0;

        long claimPages = requiredPages(rowSize);

        if (!fitsRegion(claimPages)) {
            if (bestEffort)
                return 0; // Let the page memory decide.

            throw outOfMemory();
        }

        long limitPages = limitPages(claimPages);

        // The claim is reserved before the eviction.
        long reserved = reservedPages.addAndGet(claimPages);

        boolean admitted = false;

        try {
            long deficit = deficit(limitPages, reserved);

            // An insert evicts for its own claim and restores the rest of the deficit, at most a half of the pool.
            long share = deficit < claimPages ? deficit :
                claimPages + Math.min(regCfg.getEmptyPagesPoolSize() / 2, deficit - claimPages);

            long backoffNanos = EVICTION_BACKOFF_START_NANOS;

            long lastProgressMillis = U.currentTimeMillis();

            while (deficit > 0 && share > 0) {
                if (memMetrics.onPageEvictionsStarted()) {
                    U.warn(log, "Page-based evictions started." +
                        " Consider increasing 'maxSize' on Data Region configuration: " + regCfg.getName());
                }

                if (evictDataPage()) {
                    share--;

                    if (stalled)
                        stalled = false;

                    lastProgressMillis = U.currentTimeMillis();

                    backoffNanos = EVICTION_BACKOFF_START_NANOS;
                }
                else if (bestEffort && stalled)
                    return 0;
                else if (U.currentTimeMillis() - lastProgressMillis >= noProgressTimeoutMillis) {
                    if (!bestEffort)
                        throw outOfMemory();

                    stalled = true;

                    LT.warn(log, "Page eviction made no progress, the row is inserted without a page reservation " +
                        "[region=" + regCfg.getName() + ", timeout=" + noProgressTimeoutMillis + "ms]");

                    return 0;
                }
                else {
                    LockSupport.parkNanos(backoffNanos);

                    backoffNanos = Math.min(backoffNanos << 1, EVICTION_BACKOFF_MAX_NANOS);
                }

                memMetrics.updateEvictionRate();

                deficit = deficit(limitPages, reservedPages.get());
            }

            admitted = true;

            return claimPages;
        }
        finally {
            if (!admitted)
                releaseReserved(claimPages);
        }
    }

    /** {@inheritDoc} */
    @Override public void releaseReserved(long pages) {
        assert pages >= 0;

        if (pages != 0)
            reservedPages.addAndGet(-pages);
    }

    /** @return Number of pages reserved by in-flight inserts. */
    @TestOnly
    long reservedPages() {
        return reservedPages.get();
    }

    /** @return {@code True} if the eviction is stalled: a best-effort reservation gave up because the eviction made no progress. */
    @TestOnly
    boolean stalled() {
        return stalled;
    }

    /** @return Payload of an empty data page: the size of a whole-page fragment of a row. */
    int pagePayload() {
        return pageMem.pageSize() - AbstractDataPageIO.MIN_DATA_PAGE_OVERHEAD;
    }

    /**
     * Computes the number of pages to claim for a row. The empty pages pool is not a part of the claim: it is a single
     * region-wide margin (see {@link #deficit}), so concurrent writers do not multiply it.
     *
     * @param rowSize Row size in bytes.
     * @return Number of pages required for the row.
     */
    private long requiredPages(long rowSize) {
        return (rowSize + pagePayload() - 1) / pagePayload();
    }

    /**
     * @param requiredPages Pages required for a row.
     * @return {@code True} if the row together with the empty pages pool fits into the pages the region can load.
     */
    private boolean fitsRegion(long requiredPages) {
        return requiredPages + regCfg.getEmptyPagesPoolSize() <= hardPages();
    }

    /** @return Total number of pages in the data region. */
    private long totalPages() {
        return regCfg.getMaxSize() / pageMem.systemPageSize();
    }

    /** @return Maximum number of pages the data region can load: every segment may lose a page to its overhead. */
    private long hardPages() {
        long totalPages = totalPages();

        return totalPages > SEG_CNT ? totalPages - SEG_CNT : 0;
    }

    /**
     * Computes the number of loaded pages up to which the free space of the region is counted for a row. Normally it
     * is the eviction threshold ({@link DataRegionConfiguration#getEvictionThreshold()}), so every insert path starts
     * evicting at the same fill level. A row that does not fit below the threshold together with the empty pages pool
     * may use the space above it, up to the whole region.
     *
     * @param requiredPages Pages required for the row.
     * @return Limit of loaded pages for the row.
     */
    private long limitPages(long requiredPages) {
        long hardPages = hardPages();

        long thresholdPages = Math.min((long)(regCfg.getEvictionThreshold() * totalPages()), hardPages);

        return requiredPages + regCfg.getEmptyPagesPoolSize() > thresholdPages ? hardPages : thresholdPages;
    }

    /**
     * Computes how many pages are missing: the empty pages pool and the reservations of in-flight inserts above a half
     * of it must fit into the free pages of the region (empty pages in the free list of the region plus the headroom up
     * to the limit).
     *
     * @param limitPages Limit of loaded pages (see {@link #limitPages(long)}).
     * @param reserved Pages reserved by in-flight inserts, including this claim.
     * @return Number of missing pages, a non-positive value if the claim can be admitted.
     */
    private long deficit(long limitPages, long reserved) {
        long freePages = freeList.emptyDataPages() + Math.max(0, limitPages - pageMem.loadedPages());

        return regCfg.getEmptyPagesPoolSize() + countedPages(reserved) - freePages;
    }

    /**
     * @param reserved Pages reserved by in-flight inserts.
     * @return Reserved pages above a half of the empty pages pool.
     */
    private long countedPages(long reserved) {
        return Math.max(0, reserved - regCfg.getEmptyPagesPoolSize() / 2);
    }

    /**
     * Creates an out-of-memory exception for a refused reservation. It is thrown only for inserts that can be refused
     * (see {@link #reserve(long, boolean)}) and is not reported to the failure handler: nothing has been changed yet.
     *
     * @return New {@link IgniteOutOfMemoryException} for the data region of this tracker.
     */
    private IgniteOutOfMemoryException outOfMemory() {
        return new IgniteOutOfMemoryException("Out of memory in data region [" +
            "name=" + regCfg.getName() +
            ", initSize=" + U.readableSize(regCfg.getInitialSize(), false) +
            ", maxSize=" + U.readableSize(regCfg.getMaxSize(), false) +
            ", persistenceEnabled=" + regCfg.isPersistenceEnabled() + "] Try the following:" + U.nl() +
            "  ^-- Increase maximum off-heap memory size (DataRegionConfiguration.maxSize)" + U.nl() +
            "  ^-- Enable Ignite persistence (DataRegionConfiguration.persistenceEnabled)" + U.nl() +
            "  ^-- Enable eviction or expiration policies"
        );
    }
}
