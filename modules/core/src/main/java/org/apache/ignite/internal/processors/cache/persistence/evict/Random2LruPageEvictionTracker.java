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
package org.apache.ignite.internal.processors.cache.persistence.evict;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class Random2LruPageEvictionTracker extends PageAbstractEvictionTracker {
    /** Evict attempts limit. */
    private static final int EVICT_ATTEMPTS_LIMIT = 30;

    /** LRU Sample size. */
    private static final int SAMPLE_SIZE = 5;

    /** Maximum sample search spin count */
    private static final int SAMPLE_SPIN_LIMIT = SAMPLE_SIZE * 1000;

    /** Logger. */
    private final IgniteLogger log;

    /** Tracking array ptr. */
    private long trackingArrPtr;

    /**
     * @param pageMem Page memory.
     * @param plcCfg Policy config.
     * @param sharedCtx Shared context.
     */
    public Random2LruPageEvictionTracker(
        PageMemoryNoStoreImpl pageMem,
        DataRegionConfiguration plcCfg,
        GridCacheSharedContext<?, ?> sharedCtx
    ) {
        super(pageMem, plcCfg, sharedCtx);

        DataStorageConfiguration memCfg = sharedCtx.kernalContext().config().getDataStorageConfiguration();

        assert plcCfg.getMaxSize() / memCfg.getPageSize() < Integer.MAX_VALUE;

        log = sharedCtx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        trackingArrPtr = GridUnsafe.allocateMemory(trackingSize * 8L);

        GridUnsafe.zeroMemory(trackingArrPtr, trackingSize * 8L);
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        GridUnsafe.freeMemory(trackingArrPtr);
    }

    /** {@inheritDoc} */
    @Override public void touchPage(long pageId) throws IgniteCheckedException {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        long latestTs = compactTimestamp(U.currentTimeMillis());

        assert latestTs >= 0 && latestTs < Integer.MAX_VALUE;

        boolean success;

        do {
            int trackingIdx = trackingIdx(pageIdx);

//            log.info("touchPage: latestTs=" + latestTs + ", pageIdx=" + pageIdx +
//                    ", pageId=" + PageIdUtils.pageId(pageId) + ", trackingIdx=" + trackingIdx);

            long trackingData = GridUnsafe.getLongVolatile(null, trackingArrPtr + trackingIdx * 8L);

            int firstTs = first(trackingData);

            if (firstTs < 0) {
                log.info("!!!! Touch fragment page, firstTs = " + firstTs);

                break;
            }

            int secondTs = second(trackingData);

            long newTrackData;

            if (firstTs <= secondTs)
                newTrackData = asLong((int)latestTs, secondTs);
            else
                newTrackData = asLong(firstTs, (int)latestTs);

            success = GridUnsafe.compareAndSwapLong(null, trackingArrPtr + trackingIdx * 8L, trackingData, newTrackData);
        } while (!success);
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
                int trackingIdx = rnd.nextInt(trackingSize);

                long trackData = GridUnsafe.getLongVolatile(null, trackingArrPtr + trackingIdx * 8L);

                int firstTs = first(trackData);

//                log.info(">>> evictDataPage: firstTs=" + firstTs);

                if (firstTs < 0) {
                    trackingIdx = getHeadPageTrackingIdx(trackingIdx(-firstTs));

                    trackData = GridUnsafe.getLongVolatile(null, trackingArrPtr + trackingIdx * 8L);

                    firstTs = first(trackData);
                }

                int secondTs = second(trackData);

                int minTs = Math.min(firstTs, secondTs);

                int maxTs = Math.max(firstTs, secondTs);

                if (maxTs != 0) {
                    // We chose data page with at least one touch.
                    if (minTs < lruCompactTs) {
                        lruTrackingIdx = trackingIdx;

                        lruCompactTs = minTs;
                    }

                    dataPagesCnt++;
                }

                sampleSpinCnt++;

                if (sampleSpinCnt > SAMPLE_SPIN_LIMIT) {
                    LT.warn(log, "Too many attempts to choose data page: " + SAMPLE_SPIN_LIMIT);

                    return;
                }
            }

//            log.info(">>>>> Selected page: pageIdx=" + pageIdx(lruTrackingIdx) +
//                ", pageId=" + PageIdUtils.pageId(pageIdx(lruTrackingIdx)) + ", trackingIdx=" + lruTrackingIdx);

            if (evictDataPage(pageIdx(lruTrackingIdx)))
                return;

            evictAttemptsCnt++;
        }

        LT.warn(log, "Too many failed attempts to evict page: " + EVICT_ATTEMPTS_LIMIT);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkTouch(long pageId) {
        int trackingIdx = trackingIdx(PageIdUtils.pageIndex(pageId));

        long trackingData = GridUnsafe.getLongVolatile(null, trackingArrPtr + trackingIdx * 8L);

        return first(trackingData) > 0;
    }

    /** {@inheritDoc} */
    @Override public void forgetPage(long pageId) {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        int trackingIdx = trackingIdx(pageIdx);

        GridUnsafe.putLongVolatile(null, trackingArrPtr + trackingIdx * 8L, 0L);
    }

    /** {@inheritDoc} */
    @Override public void trackFragmentPage(long pageId, long tailPageId) {
        // Store link to tail fragment page in each fragment page.
        linkFragmentPages(pageId, tailPageId);
    }

    /** {@inheritDoc} */
    @Override public void trackTailFragmentPage(long tailPageId, long headPageId) {
        // Store link to head fragment page in tail fragment page.
        linkFragmentPages(tailPageId, headPageId);
    }

    /** {@inheritDoc} */
    @Override public void forgetFragmentPage(long pageId) throws IgniteCheckedException {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        int trackingIdx = trackingIdx(pageIdx);

        long latestTs = compactTimestamp(U.currentTimeMillis());

        assert latestTs >= 0 && latestTs < Integer.MAX_VALUE;

//        log.info("unTrackFragmentPage: latestTs=" + latestTs + ", pageIdx=" + pageIdx +
//                ", pageId=" + PageIdUtils.pageId(pageId) + ", trackingIdx=" + trackingIdx);

        GridUnsafe.putLongVolatile(null, trackingArrPtr + trackingIdx * 8L, asLong((int)latestTs, 0));
    }

    /**
     * Link two pages containing fragments of row.
     * First timestamp in tracking array is set to -1. Link is stored as a page index in the second timestamp.
     *
     * @param pageId Page id.
     * @param nextPageId Page id of previous fragment.
     */
    private void linkFragmentPages(long pageId, long nextPageId) {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        int nextPageIdx = PageIdUtils.pageIndex(nextPageId);

        boolean success;

        do {
            int trackingIdx = trackingIdx(pageIdx);

            long trackinData = GridUnsafe.getLongVolatile(null, trackingArrPtr + trackingIdx * 8L);

            int firstTs = first(trackinData);

//            log.info(">>> linkFragmentPages: firstTs=" + firstTs + ", tag=" + tag +
//                ", pageIdx=" + pageIdx + ", pageId=" + PageIdUtils.pageId(pageId) + ", nextPageIdx=" + nextPageIdx
//                + " nextPageId=" + PageIdUtils.pageId(nextPageId));

            if (firstTs != 0)
                log.info("!!!! linkFragmentPages: firstTs=" + firstTs +
                    ", pageIdx=" + pageIdx + ", pageId=" + PageIdUtils.pageId(pageId) + ", nextPageIdx=" + nextPageIdx);

            if (firstTs < 0)
                return;

            success = GridUnsafe.compareAndSwapLong(null, trackingArrPtr + trackingIdx * 8L, trackinData, asLong(-nextPageIdx, 0));

            if (!success)
                log.info("!!!! linkFragmentPages: !success");
        } while (!success);
    }

    /**
     * Given tracking index of page containing any row fragment finds tracking index of the head fragment page.
     * <p>
     * Each fragment page (other than the tail one) contains link to tail page.
     * Tail page contains link to head page. So head page is found no more than in two hops.
     *
     * @param trackingIdx tracking index of page containing one of the row fragment.
     * @return tracking index of head row page.
     */
    private int getHeadPageTrackingIdx(int trackingIdx) {
        long trackingData = GridUnsafe.getLongVolatile(null, trackingArrPtr + trackingIdx * 8L);

        int firstTs = first(trackingData);

        if (firstTs < 0)
            return trackingIdx(-firstTs);
        else
            return trackingIdx;
    }

    /** */
    private static final long FIRST_INT_MASK = ~(-1L << Integer.SIZE);

    /** */
    private static final long SECOND_INT_MASK = -1L << Integer.SIZE;

    /**
     * Encode a pair of integer values into a single long.
     */
    private long asLong(int first, int second) {
        return (((long)second) << Integer.SIZE) | ((long)first & FIRST_INT_MASK);
    }

    /**
     * Decode long and extract first integer value.
     */
    private int second(long l) {
        return (int)((l & SECOND_INT_MASK) >> Integer.SIZE);
    }

    /**
     * Decode long and extract second integer value.
     */
    private int first(long l) {
        return (int)(l & FIRST_INT_MASK);
    }
}
