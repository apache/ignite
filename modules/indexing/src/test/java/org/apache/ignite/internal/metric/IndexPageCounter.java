/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.metric;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 * Helper class for counting index pages.
 */
class IndexPageCounter {
    /** Ignite node. */
    private final IgniteEx grid;

    /** {@code true} if the corresponding Ignite node is started with enabled persistence. */
    private final boolean persistenceEnabled;

    /**
     * Mapping from a page ID to the rotation id of the same page.
     * <p>
     * Needed to detect if a page got moved to a free list after some data got removed.
     */
    private final Map<Long, Long> pageIdToRotationId = new HashMap<>();

    /**
     * @param grid Grid.
     * @param persistenceEnabled Persistence enabled.
     */
    IndexPageCounter(IgniteEx grid, boolean persistenceEnabled) {
        this.grid = grid;
        this.persistenceEnabled = persistenceEnabled;
    }

    /**
     * Returns the number of index pages residing inside the Page Memory of the given cache group.
     */
    long countIdxPagesInMemory(int grpId) throws IgniteCheckedException {
        DataRegion dataRegion = grid.context().cache().context().database().dataRegion(null);
        PageMemory pageMemory = dataRegion.pageMemory();

        long idxPageCnt = 0;

        for (int i = 0; i < pageMemory.loadedPages(); i++) {
            long pageId = PageIdUtils.pageId(PageIdAllocator.INDEX_PARTITION, (byte)0, i);

            if (persistenceEnabled) {
                // if persistence is enabled, avoid loading a displaced page into memory
                PageMemoryImpl pageMemoryImpl = (PageMemoryImpl) dataRegion.pageMemory();

                if (!pageMemoryImpl.hasLoadedPage(new FullPageId(pageId, grpId)))
                    continue;
            }

            long pageAddr = pageMemory.acquirePage(grpId, pageId);

            try {
                long pageReadAddr = pageMemory.readLockForce(grpId, pageId, pageAddr);

                try {
                    // check the rotation ID in case a page was freed (its page type does not get overwritten)
                    long rotationId = PageIdUtils.rotationId(PageIO.getPageId(pageReadAddr));
                    long prevRotationId = pageIdToRotationId.computeIfAbsent(pageId, id -> rotationId);

                    if (prevRotationId == rotationId && PageIO.isIndexPage(PageIO.getType(pageReadAddr)))
                        idxPageCnt += 1;
                }
                finally {
                    pageMemory.readUnlock(grpId, pageId, pageAddr);
                }
            }
            finally {
                pageMemory.releasePage(grpId, pageId, pageAddr);
            }
        }

        return idxPageCnt;
    }
}
