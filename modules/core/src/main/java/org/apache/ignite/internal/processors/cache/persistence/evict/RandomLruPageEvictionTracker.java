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
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

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

    /** Logger. */
    private final IgniteLogger log;

    /** Tracking array ptr. */
    private long trackingArrPtr;

    /**
     * @param pageMem Page memory.
     * @param plcCfg Policy config.
     * @param sharedCtx Shared context.
     */
    public RandomLruPageEvictionTracker(
        PageMemory pageMem,
        DataRegionConfiguration plcCfg,
        GridCacheSharedContext<?, ?> sharedCtx
    ) {
        super((PageMemoryNoStoreImpl)pageMem, plcCfg, sharedCtx);

        DataStorageConfiguration memCfg = sharedCtx.kernalContext().config().getDataStorageConfiguration();

        assert plcCfg.getMaxSize() / memCfg.getPageSize() < Integer.MAX_VALUE;

        log = sharedCtx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        trackingArrPtr = GridUnsafe.allocateMemory(trackingSize * 4);

        GridUnsafe.setMemory(trackingArrPtr, trackingSize * 4, (byte)0);
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        GridUnsafe.freeMemory(trackingArrPtr);
    }

    /** {@inheritDoc} */
    @Override public void touchPage(long pageId) throws IgniteCheckedException {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        long res = compactTimestamp(U.currentTimeMillis());
        
        assert res >= 0 && res < Integer.MAX_VALUE;
        
        GridUnsafe.putIntVolatile(null, trackingArrPtr + trackingIdx(pageIdx) * 4, (int)res);
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

                int compactTs = GridUnsafe.getIntVolatile(null, trackingArrPtr + sampleTrackingIdx * 4);

                if (compactTs != 0) {
                    // We chose data page with at least one touch.
                    if (compactTs < lruCompactTs) {
                        lruTrackingIdx = sampleTrackingIdx;

                        lruCompactTs = compactTs;
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

        int ts = GridUnsafe.getIntVolatile(null, trackingArrPtr + trackingIdx * 4);

        return ts != 0;
    }

    /** {@inheritDoc} */
    @Override public void forgetPage(long pageId) {
        int pageIdx = PageIdUtils.pageIndex(pageId);

        GridUnsafe.putIntVolatile(null, trackingArrPtr + trackingIdx(pageIdx) * 4, 0);
    }
}
