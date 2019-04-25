/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.evict;

import java.util.LinkedList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;

/**
 * On-heap FIFO page eviction tracker. Only for test purposes.
 */
public class FairFifoPageEvictionTracker extends PageAbstractEvictionTracker {
    /** Page usage deque. */
    private final LinkedList<Integer> pageUsageList = new LinkedList<>();

    /**
     * @param pageMem Page memory.
     * @param plcCfg Data region configuration.
     * @param sharedCtx Shared context.
     */
    public FairFifoPageEvictionTracker(
        PageMemoryNoStoreImpl pageMem,
        DataRegionConfiguration plcCfg,
        GridCacheSharedContext sharedCtx) {
        super(pageMem, plcCfg, sharedCtx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public synchronized void touchPage(long pageId) throws IgniteCheckedException {
        pageUsageList.addLast(PageIdUtils.pageIndex(pageId));
    }

    /** {@inheritDoc} */
    @Override public synchronized void evictDataPage() throws IgniteCheckedException {
        evictDataPage(pageUsageList.pollFirst());
    }

    /** {@inheritDoc} */
    @Override public synchronized void forgetPage(long pageId) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected synchronized boolean checkTouch(long pageId) {
        return true;
    }
}
