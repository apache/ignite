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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;

/**
 * Merge on remove.
 */
public class MergeRecord<L> extends PageDeltaRecord {
    /** */
    private long prntId;

    /** */
    private long rightId;

    /** */
    private int prntIdx;

    /** */
    private boolean emptyBranch;

    /**
     * @param cacheId Cache ID.
     * @param pageId  Page ID.
     * @param prntId Parent ID.
     * @param prntIdx Index in parent page.
     * @param rightId Right ID.
     * @param emptyBranch We are merging empty branch.
     */
    public MergeRecord(int cacheId, long pageId, long prntId, int prntIdx, long rightId, boolean emptyBranch) {
        super(cacheId, pageId);

        this.prntId = prntId;
        this.rightId = rightId;
        this.prntIdx = prntIdx;
        this.emptyBranch = emptyBranch;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(GridCacheContext<?,?> cctx, ByteBuffer leftBuf) throws IgniteCheckedException {
        BPlusIO<L> io = PageIO.getBPlusIO(leftBuf);

        PageMemory m = cctx.shared().database().pageMemory();

        try (
            Page prnt = m.page(cacheId(), prntId);
            Page right = m.page(cacheId(), rightId)
        ) {
            ByteBuffer prntBuf = prnt.getForRead();

            try {
                BPlusIO<L> prntIo = PageIO.getBPlusIO(prntBuf);

                ByteBuffer rightBuf = right.getForRead();

                try {
                    if (!io.merge(prntIo, prntBuf, prntIdx, leftBuf, rightBuf, emptyBranch))
                        throw new DeltaApplicationException("Failed to merge page.");
                }
                finally {
                    right.releaseRead();
                }
            }
            finally {
                prnt.releaseRead();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_PAGE_MERGE;
    }

    public long parentId() {
        return prntId;
    }

    public int parentIndex() {
        return prntIdx;
    }

    public long rightId() {
        return rightId;
    }

    public boolean isEmptyBranch() {
        return emptyBranch;
    }
}
