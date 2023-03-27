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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.PageReplacementMode;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionCountersIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.META_PAGE_ID;
import static org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl.INVALID_REL_PTR;
import static org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl.PAGE_OVERHEAD;

/**
 * Random-LRU page replacement policy implementation.
 *
 * @see PageReplacementMode#RANDOM_LRU
 */
public class RandomLruPageReplacementPolicy extends PageReplacementPolicy {
    /** Number of random pages that will be picked for eviction. */
    public static final int RANDOM_PAGES_EVICT_NUM = 5;

    /** */
    private static final double FULL_SCAN_THRESHOLD = 0.4;

    /**
     * @param seg Page memory segment.
     */
    protected RandomLruPageReplacementPolicy(PageMemoryImpl.Segment seg) {
        super(seg);
    }

    /** {@inheritDoc} */
    @Override public long replace() throws IgniteCheckedException {
        final ThreadLocalRandom rnd = ThreadLocalRandom.current();

        LoadedPagesMap loadedPages = seg.loadedPages();
        PagePool pool = seg.pool();

        final int cap = loadedPages.capacity();

        // With big number of random picked pages we may fall into infinite loop, because
        // every time the same page may be found.
        Set<Long> ignored = null;

        long relRmvAddr = INVALID_REL_PTR;

        int iterations = 0;

        while (true) {
            long cleanAddr = INVALID_REL_PTR;
            long cleanTs = Long.MAX_VALUE;
            long dirtyAddr = INVALID_REL_PTR;
            long dirtyTs = Long.MAX_VALUE;
            long metaAddr = INVALID_REL_PTR;
            long metaTs = Long.MAX_VALUE;

            for (int i = 0; i < RANDOM_PAGES_EVICT_NUM; i++) {
                ++iterations;

                if (iterations > pool.pages() * FULL_SCAN_THRESHOLD)
                    break;

                // We need to lookup for pages only in current segment for thread safety,
                // so peeking random memory will lead to checking for found page segment.
                // It's much faster to check available pages for segment right away.
                ReplaceCandidate nearest = loadedPages.getNearestAt(rnd.nextInt(cap));

                assert nearest != null && nearest.relativePointer() != INVALID_REL_PTR;

                long rndAddr = nearest.relativePointer();

                int partGen = nearest.generation();

                final long absPageAddr = seg.absolute(rndAddr);

                FullPageId fullId = PageHeader.fullPageId(absPageAddr);

                // Check page mapping consistency.
                assert fullId.equals(nearest.fullId()) : "Invalid page mapping [tableId=" + nearest.fullId() +
                    ", actual=" + fullId + ", nearest=" + nearest;

                boolean outdated = partGen < seg.partGeneration(fullId.groupId(), PageIdUtils.partId(fullId.pageId()));

                if (outdated)
                    return seg.refreshOutdatedPage(fullId.groupId(), fullId.pageId(), true);

                boolean pinned = PageHeader.isAcquired(absPageAddr);

                boolean skip = ignored != null && ignored.contains(rndAddr);

                final boolean dirty = PageHeader.dirty(absPageAddr);

                CheckpointPages checkpointPages = seg.checkpointPages();

                if (relRmvAddr == rndAddr || pinned || skip ||
                    fullId.pageId() == META_PAGE_ID ||
                    (dirty && (checkpointPages == null || !checkpointPages.contains(fullId)))
                ) {
                    i--;

                    continue;
                }

                final long pageTs = PageHeader.readTimestamp(absPageAddr);

                final boolean storMeta = isStoreMetadataPage(absPageAddr);

                if (pageTs < cleanTs && !dirty && !storMeta) {
                    cleanAddr = rndAddr;

                    cleanTs = pageTs;
                }
                else if (pageTs < dirtyTs && dirty && !storMeta) {
                    dirtyAddr = rndAddr;

                    dirtyTs = pageTs;
                }
                else if (pageTs < metaTs && storMeta) {
                    metaAddr = rndAddr;

                    metaTs = pageTs;
                }

                if (cleanAddr != INVALID_REL_PTR)
                    relRmvAddr = cleanAddr;
                else if (dirtyAddr != INVALID_REL_PTR)
                    relRmvAddr = dirtyAddr;
                else
                    relRmvAddr = metaAddr;
            }

            if (relRmvAddr == INVALID_REL_PTR)
                return tryToFindSequentially(cap);

            final long absRmvAddr = seg.absolute(relRmvAddr);

            final FullPageId fullPageId = PageHeader.fullPageId(absRmvAddr);

            if (!seg.tryToRemovePage(fullPageId, absRmvAddr)) {
                if (iterations > 10) {
                    if (ignored == null)
                        ignored = new HashSet<>();

                    ignored.add(relRmvAddr);
                }

                if (iterations > seg.pool().pages() * FULL_SCAN_THRESHOLD)
                    return tryToFindSequentially(cap);

                continue;
            }

            return relRmvAddr;
        }
    }

    /**
     * @param absPageAddr Absolute page address
     * @return {@code True} if page is related to partition metadata, which is loaded in saveStoreMetadata().
     */
    private static boolean isStoreMetadataPage(long absPageAddr) {
        try {
            long dataAddr = absPageAddr + PAGE_OVERHEAD;

            int type = PageIO.getType(dataAddr);
            int ver = PageIO.getVersion(dataAddr);

            PageIO io = PageIO.getPageIO(type, ver);

            return io instanceof PagePartitionMetaIO
                || io instanceof PagesListMetaIO
                || io instanceof PagePartitionCountersIO;
        }
        catch (IgniteCheckedException ignored) {
            return false;
        }
    }

    /**
     * Will scan all segment pages to find one to evict it.
     *
     * @param cap Capacity.
     */
    private long tryToFindSequentially(int cap) throws IgniteCheckedException {
        assert seg.getWriteHoldCount() > 0;

        long prevAddr = INVALID_REL_PTR;

        LoadedPagesMap loadedPages = seg.loadedPages();

        for (int i = 0; i < cap; i++) {
            final ReplaceCandidate nearest = loadedPages.getNearestAt(i);

            assert nearest != null && nearest.relativePointer() != INVALID_REL_PTR;

            final long addr = nearest.relativePointer();

            int partGen = nearest.generation();

            final long absPageAddr = seg.absolute(addr);

            FullPageId fullId = PageHeader.fullPageId(absPageAddr);

            if (partGen < seg.partGeneration(fullId.groupId(), PageIdUtils.partId(fullId.pageId())))
                return seg.refreshOutdatedPage(fullId.groupId(), fullId.pageId(), true);

            boolean pinned = PageHeader.isAcquired(absPageAddr);

            if (addr == prevAddr || pinned)
                continue;

            final long absEvictAddr = seg.absolute(addr);

            final FullPageId fullPageId = PageHeader.fullPageId(absEvictAddr);

            if (seg.tryToRemovePage(fullPageId, absEvictAddr))
                return addr;

            prevAddr = addr;
        }

        throw seg.oomException("no pages to replace");
    }
}
