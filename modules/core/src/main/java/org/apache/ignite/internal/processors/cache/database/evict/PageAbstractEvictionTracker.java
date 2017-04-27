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
package org.apache.ignite.internal.processors.cache.database.evict;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.database.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public abstract class PageAbstractEvictionTracker implements PageEvictionTracker {
    /** This number of least significant bits is dropped from timestamp. */
    private static final int COMPACT_TS_SHIFT = 8; // Enough if grid works for less than 17 years.

    /** Millis in day. */
    private static final int DAY = 24 * 60 * 60 * 1000;

    /** Page memory. */
    protected final PageMemory pageMem;

    /** Tracking array size. */
    protected final int trackingSize;

    /** Base compact timestamp. */
    private final long baseCompactTs;

    /** Shared context. */
    private final GridCacheSharedContext sharedCtx;

    /* TODO: IGNITE-4921: Will be removed after segments refactoring >>>> */
    protected final int segBits;
    protected final int idxBits;
    protected final int segMask;
    protected final int idxMask;
    protected final int segmentPageCount;
    /* <<<< */

    /**
     * @param pageMem Page memory.
     * @param plcCfg Memory policy configuration.
     * @param sharedCtx Shared context.
     */
    PageAbstractEvictionTracker(
        PageMemory pageMem,
        MemoryPolicyConfiguration plcCfg,
        GridCacheSharedContext sharedCtx
    ) {
        this.pageMem = pageMem;

        this.sharedCtx = sharedCtx;

        MemoryConfiguration memCfg = sharedCtx.kernalContext().config().getMemoryConfiguration();

        /* TODO: IGNITE-4921: Will be removed after segments refactoring >>>> */
        int concurrencyLevel = memCfg.getConcurrencyLevel();

        if (concurrencyLevel < 1)
            concurrencyLevel = Runtime.getRuntime().availableProcessors();

        int pageSize = memCfg.getPageSize();

        long segSize = plcCfg.getSize() / concurrencyLevel;

        if (segSize < 1024 * 1024)
            segSize = 1024 * 1024;

        segmentPageCount = (int)(segSize / pageSize);

        segBits = Integer.SIZE - Integer.numberOfLeadingZeros(concurrencyLevel - 1);

        idxBits = PageIdUtils.PAGE_IDX_SIZE - segBits;

        segMask = ~(-1 << segBits);

        idxMask = ~(-1 << idxBits);
        /* <<<< */

        trackingSize = segmentPageCount << segBits;

        baseCompactTs = (U.currentTimeMillis() - DAY) >> COMPACT_TS_SHIFT;
        // We subtract day to avoid fail in case of daylight shift or timezone change.
    }

    /**
     * @param pageIdx Page index.
     * @return true if at least one data row has been evicted
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

                        row.initFromLink(null, sharedCtx, pageMem, CacheDataRowAdapter.RowData.KEY_ONLY);

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

        boolean evictionDone = false;

        for (CacheDataRowAdapter dataRow : rowsToEvict) {
            GridCacheContext<?, ?> cacheCtx = sharedCtx.cacheContext(dataRow.cacheId());

            if (!cacheCtx.userCache())
                continue;

            GridCacheEntryEx entryEx = cacheCtx.isNear() ? cacheCtx.near().dht().entryEx(dataRow.key()) :
                cacheCtx.cache().entryEx(dataRow.key());

            evictionDone |= entryEx.evictInternal(GridCacheVersionManager.EVICT_VER, null, true);
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
        int inSegmentPageIdx = inSegmentPageIdx(pageIdx);

        assert inSegmentPageIdx < segmentPageCount : inSegmentPageIdx;

        int trackingIdx = segmentIdx(pageIdx) * segmentPageCount + inSegmentPageIdx;

        assert trackingIdx < trackingSize : trackingIdx;

        return trackingIdx;
    }

    /**
     * Reverse of {@link #trackingIdx(int)}.
     *
     * @param trackingIdx Tracking index.
     * @return Page index.
     */
    int pageIdx(int trackingIdx) {
        assert trackingIdx < trackingSize;

        long res = 0;

        long segIdx = trackingIdx / segmentPageCount;
        long pageIdx = trackingIdx % segmentPageCount;

        res = (res << segBits) | (segIdx & segMask);
        res = (res << idxBits) | (pageIdx & idxMask);

        assert (res & (-1L << 32)) == 0 : res;

        return (int)res;
    }

    /* TODO: IGNITE-4921: Will be removed after segments refactoring >>>> */
    /**
     * @param pageIdx Page index.
     * @return Number of segment.
     */
    private int segmentIdx(int pageIdx) {
        return (pageIdx >> idxBits) & segMask;
    }

    /**
     * @param pageIdx Page index.
     * @return Number of page inside segment.
     */
    private int inSegmentPageIdx(int pageIdx) {
        return pageIdx & idxMask;
    }
    /* <<<< */
}
