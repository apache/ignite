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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.util.typedef.F;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.jsr166.ConcurrentLinkedHashMap.DFLT_INIT_CAP;
import static org.jsr166.ConcurrentLinkedHashMap.DFLT_LOAD_FACTOR;

/**
 * H2 row cache.
 */
public class H2RowCache implements GridQueryRowCacheCleaner {
    /** Cached rows. */
    private final ConcurrentLinkedHashMap<Long, H2CacheRow> rows;

    /** Cache group ID. */
    private final CacheGroupContext grpCtx;

    /** Usage count. */
    private int usageCnt = 1;

    /**
     * @param grpCtx Cache group context.
     */
    public H2RowCache(CacheGroupContext grpCtx, int maxSize) {
        this.grpCtx = grpCtx;

        rows = new ConcurrentLinkedHashMap<>(
            DFLT_INIT_CAP,
            DFLT_LOAD_FACTOR,
            Runtime.getRuntime().availableProcessors(),
            maxSize
        );
    }

    /**
     * Get row by link.
     *
     * @param link Link.
     * @return Cached on-heap row.
     * @throws IgniteCheckedException On error.
     */
    public H2CacheRow get(long link) throws IgniteCheckedException {
        H2CacheRow row = rows.get(link);

        if (row != null)
            touch(link);

        return row;
    }

    /**
     * Put row by link.
     *
     * @param row Row.
     */
    public void put(H2CacheRow row) {
        rows.put(row.link(), row);
    }

    /** {@inheritDoc} */
    @Override public void remove(long link) {
        rows.remove(link);
    }

    /**
     * Cache registration callback.
     */
    public void onCacheRegistered() {
        usageCnt++;
    }

    /**
     * Cache un-registration callback.
     *
     * @param cacheInfo Cache context info.
     * @return {@code True} if there are no more usages for the given cache group.
     */
    public boolean onCacheUnregistered(GridCacheContextInfo cacheInfo) {
        boolean res = --usageCnt == 0;

        clearForCache(cacheInfo);

        return res;
    }

    /**
     * @return Cached rows count.
     */
    public int size() {
        return rows.size();
    }

    /**
     * Clear entries belonging to the given cache.
     *
     * @param cacheInfo Cache context info.
     */
    private void clearForCache(GridCacheContextInfo cacheInfo) {
        int cacheId = cacheInfo.cacheId();

        Iterator<Map.Entry<Long, H2CacheRow>> iter = rows.entrySet().iterator();

        while (iter.hasNext()) {
            H2CacheRow row = iter.next().getValue();

            if (F.eq(cacheId, row.cacheId()))
                iter.remove();
        }
    }

    /**
     * Update page
     *
     * @param link Link.
     * @throws IgniteCheckedException On error.
     */
    private void touch(long link) throws IgniteCheckedException {
        PageMemory mem = grpCtx.dataRegion().pageMemory();

        int grpId = grpCtx.groupId();

        final long pageId = PageIdUtils.pageId(link);

        final long page = mem.acquirePage(grpId, pageId);

        try {
            // Touch page timestamp
            mem.readLock(grpId, pageId, page);

            mem.readUnlock(grpId, pageId, page);
        }
        finally {
            mem.releasePage(grpId, pageId, page);
        }
    }
}
