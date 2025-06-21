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

            long trackingData = GridUnsafe.getLongVolatile(null, trackingArrPtr + trackingIdx * 8L);

            int firstTs = first(trackingData);

            assert firstTs >= 0 : "[firstTs=" + firstTs + ", trackingData=" + trackingData + "]";

            int secondTs = second(trackingData);

            long newTrackingData;

            if (firstTs <= secondTs)
                newTrackingData = U.toLong((int)latestTs, secondTs);
            else
                newTrackingData = U.toLong(firstTs, (int)latestTs);

            success = GridUnsafe.compareAndSwapLong(null, trackingArrPtr + trackingIdx * 8L, trackingData, newTrackingData);
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

                long trackingData = GridUnsafe.getLongVolatile(null, trackingArrPtr + trackingIdx * 8L);

                int firstTs = first(trackingData);

                if (firstTs < 0) {
                    // For page containing fragmented row data timestamps stored in the row's head page are used.
                    // Fragment page (other than the tail one) contains link to tail page. Tail page contains link to
                    // head page. So head page is found no more than in two hops.
                    trackingIdx = -firstTs;

                    trackingData = GridUnsafe.getLongVolatile(null, trackingArrPtr + trackingIdx * 8L);

                    firstTs = first(trackingData);

                    if (firstTs < 0) {
                        trackingIdx = -firstTs;

                        trackingData = GridUnsafe.getLongVolatile(null, trackingArrPtr + trackingIdx * 8L);

                        firstTs = first(trackingData);

                        assert firstTs >= 0 : "[firstTs=" + firstTs + ", secondTs=" + second(trackingData) + "]";
                    }
                }

                int secondTs = second(trackingData);

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

            if (evictDataPage(pageIdx(lruTrackingIdx)))
                return;

            evictAttemptsCnt++;
        }

        LT.warn(log, "Too many failed attempts to evict page: " + EVICT_ATTEMPTS_LIMIT);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkTouch(long pageId) {
        int trackingIdx = trackingIdx(PageIdUtils.pageIndex(pageId));

        int firstTs = first(GridUnsafe.getLongVolatile(null, trackingArrPtr + trackingIdx * 8L));

        return firstTs > 0;
    }

    /** {@inheritDoc} */
    @Override public void forgetPage(long pageId) {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        int trackingIdx = trackingIdx(pageIdx);

        GridUnsafe.putLongVolatile(null, trackingArrPtr + trackingIdx * 8L, 0L);
    }

    /**
     * Link two pages containing fragments of row.
     * <p>
     * Link is stored as a negated page tracking index in the first integer in the tracking data.
     * Second integer in the tracking data is set to 0.
     *
     * @param pageTrackingIdx     Page tracking index.
     * @param nextPageTrackingIdx Tracking index of page to link to.
     */
    @Override protected void linkFragmentPages(int pageTrackingIdx, int nextPageTrackingIdx) {
        GridUnsafe.putLongVolatile(null, trackingArrPtr + pageTrackingIdx * 8L,
            U.toLong(-nextPageTrackingIdx, 0));
    }

    /** {@inheritDoc} */
    @Override protected int getFragmentLink(int pageTrackingIdx) {
        int link = first(GridUnsafe.getLongVolatile(null, trackingArrPtr + pageTrackingIdx * 8L));

        assert link <= 0 : "[link=" + link + "]";

        return -link;
    }

    /**
     * Helper to extract first integer value.
     */
    private int first(long l) {
        return (int)(l >> Integer.SIZE);
    }

    /**
     * Helper to extract second integer value.
     */
    private int second(long l) {
        return (int)l;
    }
}
