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

package org.apache.ignite.internal.processors.cache.persistence.partstorage;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.IncompleteObject;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;

import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 */
public class PartitionMetaStorageImpl<T extends Storable> extends AbstractFreeList<T> implements PartitionMetaStorage<T> {
    /**
     * @param cacheId Cache id.
     * @param name Name.
     * @param memMetrics Mem metrics.
     * @param memPlc Mem policy.
     * @param reuseList Reuse list.
     * @param wal Wal.
     * @param metaPageId Meta page id.
     * @param initNew Initialize new.
     */
    public PartitionMetaStorageImpl(
        int cacheId, String name,
        DataRegionMetricsImpl memMetrics,
        DataRegion memPlc,
        ReuseList reuseList,
        IgniteWriteAheadLogManager wal,
        long metaPageId,
        boolean initNew,
        PageLockListener lsnr,
        GridKernalContext ctx,
        AtomicLong pageListCacheLimit
    ) throws IgniteCheckedException {
        super(cacheId, name, memMetrics, memPlc, reuseList, wal, metaPageId, initNew, lsnr, ctx, pageListCacheLimit);
    }

    /**
     * Read row as byte array from data pages.
     */
    @Override public final byte[] readRow(long link) throws IgniteCheckedException {
        assert link != 0 : "link";

        long nextLink = link;
        IncompleteObject incomplete = null;
        int size = 0;

        boolean first = true;

        do {
            final long pageId = pageId(nextLink);

            final long page = pageMem.acquirePage(grpId, pageId);

            try {
                long pageAddr = pageMem.readLock(grpId, pageId, page); // Non-empty data page must not be recycled.

                assert pageAddr != 0L : nextLink;

                try {
                    AbstractDataPageIO io = PageIO.getPageIO(pageAddr);

                    //MetaStorage never encrypted so realPageSize == pageSize.
                    DataPagePayload data = io.readPayload(pageAddr, itemId(nextLink), pageMem.pageSize());

                    nextLink = data.nextLink();

                    if (first) {
                        if (nextLink == 0) {
                            long payloadAddr = pageAddr + data.offset();

                            // Fast path for a single page row.
                            return PageUtils.getBytes(payloadAddr, 4, PageUtils.getInt(payloadAddr, 0));
                        }

                        first = false;
                    }

                    ByteBuffer buf = pageMem.pageBuffer(pageAddr);

                    buf.position(data.offset());
                    buf.limit(data.offset() + data.payloadSize());

                    if (size == 0) {
                        if (buf.remaining() >= 4 && incomplete == null) {
                            // Just read size.
                            size = buf.getInt();
                            incomplete = new IncompleteObject(new byte[size]);
                        }
                        else {
                            if (incomplete == null)
                                incomplete = new IncompleteObject(new byte[4]);

                            incomplete.readData(buf);

                            if (incomplete.isReady()) {
                                size = ByteBuffer.wrap(incomplete.data()).order(buf.order()).getInt();
                                incomplete = new IncompleteObject(new byte[size]);
                            }
                        }
                    }

                    if (size != 0 && buf.remaining() > 0)
                        incomplete.readData(buf);
                }
                finally {
                    pageMem.readUnlock(grpId, pageId, page);
                }
            }
            finally {
                pageMem.releasePage(grpId, pageId, page);
            }
        }
        while (nextLink != 0);

        assert incomplete.isReady();

        return incomplete.data();
    }
}
