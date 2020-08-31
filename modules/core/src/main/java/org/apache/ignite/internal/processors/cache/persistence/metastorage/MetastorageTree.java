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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;

/**
 *
 */
public class MetastorageTree extends BPlusTree<MetastorageRow, MetastorageDataRow> {
    /** Max key length (bytes num) */
    public static final int MAX_KEY_LEN = 64;

    /** */
    private MetastorageRowStore rowStore;

    /** Partition id. */
    private final int partId;

    /**
     * @param pageMem Page memory instance.
     * @param wal WAL manager.
     * @param globalRmvId Global remove ID.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param rowStore Row store.
     * @param initNew Init new flag, if {@code true}, then new tree will be allocated.
     * @param failureProcessor To call if the tree is corrupted.
     * @throws IgniteCheckedException If failed to initialize.
     */
    public MetastorageTree(
        int cacheId,
        String name,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        ReuseList reuseList,
        MetastorageRowStore rowStore,
        long metaPageId,
        boolean initNew,
        @Nullable FailureProcessor failureProcessor,
        int partId,
        @Nullable PageLockListener lockLsnr
    ) throws IgniteCheckedException {
        super(
            name,
            cacheId,
            null,
            pageMem,
            wal,
            globalRmvId,
            metaPageId,
            reuseList,
            MetastorageBPlusIO.INNER_IO_VERSIONS,
            MetastorageBPlusIO.LEAF_IO_VERSIONS,
            failureProcessor,
            lockLsnr
        );

        this.rowStore = rowStore;

        this.partId = partId;

        initTree(initNew);
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<MetastorageRow> io, long pageAddr, int idx,
        MetastorageRow row) throws IgniteCheckedException {

        String key = ((MetastorageBPlusIO)io).getKey(pageAddr, idx, rowStore);

        return key.compareTo(row.key());
    }

    /** {@inheritDoc} */
    @Override public MetastorageDataRow getRow(BPlusIO<MetastorageRow> io, long pageAddr, int idx,
        Object x) throws IgniteCheckedException {

        return ((MetastorageBPlusIO)io).getDataRow(pageAddr, idx, rowStore);
    }

    /**
     * @return RowStore.
     */
    public MetastorageRowStore rowStore() {
        return rowStore;
    }

    /** {@inheritDoc} */
    @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
        return pageMem.allocatePage(grpId, partId, FLAG_DATA);
    }
}
