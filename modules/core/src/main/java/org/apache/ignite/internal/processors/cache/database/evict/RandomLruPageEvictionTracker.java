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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 *
 */
public class RandomLruPageEvictionTracker extends PageAbstractEvictionTracker {
    /** Evict attempts limit. */
    private static final int EVICT_ATTEMPTS_LIMIT = 30;

    /** LRU Sample size. */
    private static final int SAMPLE_SIZE = 5;

    /** Maximum sample search spin count */
    private static final int SAMPLE_SPIN_LIMIT = SAMPLE_SIZE * 1000;

    /** This number of least significant bits is dropped from timestamp. */
    private static final int COMPACT_TS_SHIFT = 8; // Enough if grid works for less than 17 years.

    /** Millis in day. */
    private final static int DAY = 24 * 60 * 60 * 1000;

    /** Tracking array ptr. */
    private final long trackingArrPtr;

    /** Base compact timestamp. */
    private final long baseCompactTs;

    /** Tracking array size. */
    private final int trackingSize;

    /**
     * @param pageMem Page mem.
     * @param sharedCtx Shared context.
     */
    public RandomLruPageEvictionTracker(PageMemory pageMem, GridCacheSharedContext sharedCtx) {
        super(pageMem, sharedCtx);

        MemoryConfiguration memCfg = sharedCtx.kernalContext().config().getMemoryConfiguration();

        baseCompactTs = (System.currentTimeMillis() - DAY) >> COMPACT_TS_SHIFT;
        // We subtract day to avoid fail in case of daylight shift or timezone change.

        assert memCfg.getPageCacheSize() / memCfg.getPageSize() < Integer.MAX_VALUE;

        trackingSize = segmentPageCount << segBits;

        trackingArrPtr = GridUnsafe.allocateMemory(trackingSize * 4);

        GridUnsafe.setMemory(trackingArrPtr, trackingSize * 4, (byte)0);
    }

    /** {@inheritDoc} */
    @Override public void touchPage(long pageId) throws IgniteCheckedException {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        long res = compactTimestamp(System.currentTimeMillis());
        
        assert res >= 0 && res < Integer.MAX_VALUE;
        
        GridUnsafe.putInt(trackingArrPtr + trackingIdx(pageIdx) * 4, (int)res);
    }

    /** {@inheritDoc} */
    @Override public void evictDataPage() throws IgniteCheckedException {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        int evictAttemptsCnt = 0;

        while (evictAttemptsCnt < EVICT_ATTEMPTS_LIMIT) {

            int lruTrackingIdx = -1;

            int lruCompactTs = Integer.MAX_VALUE;

            int dataPagesCnt = 0;

            int sampleSpinCnt = 0;

            while (dataPagesCnt < SAMPLE_SIZE) {
                int sampleTrackingIdx = rnd.nextInt(trackingSize);

                int compactTs = GridUnsafe.getInt(trackingArrPtr + sampleTrackingIdx * 4);

                if (compactTs != 0) {
                    // We chose data page with at least one touch.
                    if (compactTs < lruCompactTs) {
                        lruTrackingIdx = sampleTrackingIdx;

                        lruCompactTs = compactTs;
                    }

                    dataPagesCnt++;
                }

                sampleSpinCnt++;

                if (sampleSpinCnt > SAMPLE_SPIN_LIMIT)
                    throw new IgniteCheckedException("Too many attempts to choose data page: " + SAMPLE_SPIN_LIMIT);
            }

            if (evictDataPage(pageIdx(lruTrackingIdx)))
                return;

            evictAttemptsCnt++;
        }

        throw new IgniteCheckedException("Too many failed attempts to evict page: " + EVICT_ATTEMPTS_LIMIT);
    }

    /**
     * @param epochMilli Time millis.
     */
    private long compactTimestamp(long epochMilli) {
        return (epochMilli >> COMPACT_TS_SHIFT) - baseCompactTs;
    }

    /**
     * Resolves position in tracking array by page index.
     *
     * @param pageIdx Page index.
     */
    private int trackingIdx(int pageIdx) {
        int inSegmentPageIdx = inSegmentPageIdx(pageIdx);

        assert inSegmentPageIdx < segmentPageCount;

        int trackingIdx = segmentIdx(pageIdx) * segmentPageCount + inSegmentPageIdx;

        assert trackingIdx < trackingSize;

        return trackingIdx;
    }
    
    /**
     * Reverse of {@link #trackingIdx(int)}.
     *
     * @param trackingIdx Tracking index.
     */
    private int pageIdx(int trackingIdx) {
        assert trackingIdx < trackingSize;

        long res = 0;
        
        long segIdx = trackingIdx / segmentPageCount;
        long pageIdx = trackingIdx % segmentPageCount;

        res = (res << segBits) | (segIdx & segMask);
        res = (res << idxBits) | (pageIdx & idxMask);
        
        assert (res & (-1L << 32)) == 0;

        return (int)res;
    }
}
