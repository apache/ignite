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

package org.apache.ignite.internal.processors.cache.mvcc.txlog;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRecord;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

/**
 *
 */
public class TxLog implements DbCheckpointListener {
    /** */
    public static final String TX_LOG_CACHE_NAME = "TxLog";

    /** */
    public static final int TX_LOG_CACHE_ID = CU.cacheId(TX_LOG_CACHE_NAME);

    /** */
    private static final TxSearchRow LOWEST = new TxSearchRow(0, 0);

    /** */
    private final IgniteCacheDatabaseSharedManager mgr;

    /** */
    private ReuseListImpl reuseList;

    /** */
    private TxLogTree tree;

    /**
     *
     * @param ctx Kernal context.
     * @param mgr Database shared manager.
     */
    public TxLog(GridKernalContext ctx, IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
        this.mgr = mgr;

        init(ctx);
    }

    /**
     *
     * @param ctx Kernal context.
     * @throws IgniteCheckedException If failed.
     */
    private void init(GridKernalContext ctx) throws IgniteCheckedException {
        if (CU.isPersistenceEnabled(ctx.config())) {
            mgr.checkpointReadLock();

            try {
                IgniteWriteAheadLogManager wal = ctx.cache().context().wal();
                PageMemoryEx pageMemory = (PageMemoryEx)mgr.dataRegion(TX_LOG_CACHE_NAME).pageMemory();

                long partMetaId = pageMemory.partitionMetaPageId(TX_LOG_CACHE_ID, 0);
                long partMetaPage = pageMemory.acquirePage(TX_LOG_CACHE_ID, partMetaId);

                long treeRoot, reuseListRoot;

                boolean isNew = false;

                try {
                    long pageAddr = pageMemory.writeLock(TX_LOG_CACHE_ID, partMetaId, partMetaPage);

                    try {
                        if (PageIO.getType(pageAddr) != PageIO.T_PART_META) {
                            // Initialize new page.
                            PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.latest();

                            io.initNewPage(pageAddr, partMetaId, pageMemory.pageSize());

                            treeRoot = pageMemory.allocatePage(TX_LOG_CACHE_ID, 0, PageMemory.FLAG_DATA);
                            reuseListRoot = pageMemory.allocatePage(TX_LOG_CACHE_ID, 0, PageMemory.FLAG_DATA);

                            assert PageIdUtils.flag(treeRoot) == PageMemory.FLAG_DATA;
                            assert PageIdUtils.flag(reuseListRoot) == PageMemory.FLAG_DATA;

                            io.setTreeRoot(pageAddr, treeRoot);
                            io.setReuseListRoot(pageAddr, reuseListRoot);

                            if (PageHandler.isWalDeltaRecordNeeded(pageMemory, TX_LOG_CACHE_ID, partMetaId, partMetaPage, wal, null))
                                wal.log(new MetaPageInitRecord(
                                    TX_LOG_CACHE_ID,
                                    partMetaId,
                                    io.getType(),
                                    io.getVersion(),
                                    treeRoot,
                                    reuseListRoot
                                ));

                            isNew = true;
                        }
                        else {
                            PagePartitionMetaIO io = PageIO.getPageIO(pageAddr);

                            treeRoot = io.getTreeRoot(pageAddr);
                            reuseListRoot = io.getReuseListRoot(pageAddr);

                            assert PageIdUtils.flag(treeRoot) == PageMemory.FLAG_DATA :
                                U.hexLong(treeRoot) + ", part=" + 0 + ", TX_LOG_CACHE_ID=" + TX_LOG_CACHE_ID;
                            assert PageIdUtils.flag(reuseListRoot) == PageMemory.FLAG_DATA :
                                U.hexLong(reuseListRoot) + ", part=" + 0 + ", TX_LOG_CACHE_ID=" + TX_LOG_CACHE_ID;
                        }
                    }
                    finally {
                        pageMemory.writeUnlock(TX_LOG_CACHE_ID, partMetaId, partMetaPage, null, isNew);
                    }
                }
                finally {
                    pageMemory.releasePage(TX_LOG_CACHE_ID, partMetaId, partMetaPage);
                }

                reuseList = new ReuseListImpl(
                    TX_LOG_CACHE_ID,
                    TX_LOG_CACHE_NAME,
                    pageMemory,
                    wal,
                    reuseListRoot,
                    isNew);

                tree = new TxLogTree(pageMemory, wal, treeRoot, reuseList, isNew);

                ((GridCacheDatabaseSharedManager)mgr).addCheckpointListener(this);
            }
            finally {
                mgr.checkpointReadUnlock();
            }
        }
        else {
            PageMemory pageMemory = mgr.dataRegion(TX_LOG_CACHE_NAME).pageMemory();
            ReuseList reuseList1 = mgr.reuseList(TX_LOG_CACHE_NAME);

            long treeRoot;

            if ((treeRoot = reuseList1.takeRecycledPage()) == 0L)
                treeRoot = pageMemory.allocatePage(TX_LOG_CACHE_ID, INDEX_PARTITION, FLAG_IDX);

            tree = new TxLogTree(pageMemory, null, treeRoot, reuseList1, true);
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
        reuseList.saveMetadata();
    }

    /**
     *
     * @param major Major version.
     * @param minor Minor version.
     * @return Transaction state for given version.
     * @throws IgniteCheckedException If failed.
     */
    public byte get(long major, long minor) throws IgniteCheckedException {
        mgr.checkpointReadLock();

        try {
            TxRow row = tree.findOne(new TxSearchRow(major, minor));

            return row == null ? TxState.NA : row.state();
        }
        finally {
            mgr.checkpointReadUnlock();
        }
    }

    /**
     *
     * @param major Major version.
     * @param minor Minor version.
     * @param state  Transaction state for given version.
     * @throws IgniteCheckedException If failed.
     */
    public void put(long major, long minor, byte state) throws IgniteCheckedException {
        mgr.checkpointReadLock();

        try {
            tree.putx(new TxRow(major, minor, state));
        }
        finally {
            mgr.checkpointReadUnlock();
        }
    }

    /**
     * Removes all records less or equals to the given version.
     *
     * @param major Major version.
     * @param minor Minor version.
     * @throws IgniteCheckedException If failed.
     */
    public void removeUntil(long major, long minor) throws IgniteCheckedException {
        mgr.checkpointReadLock();

        try {
            TraversingClosure clo = new TraversingClosure(major, minor);

            tree.iterate(LOWEST, clo, clo);

            if (clo.rows != null) {
                for (TxSearchRow row : clo.rows) {
                    tree.removex(row);
                }
            }
        }
        finally {
            mgr.checkpointReadUnlock();
        }
    }

    /**
     *
     */
    private static class TraversingClosure extends TxSearchRow implements BPlusTree.TreeRowClosure<TxSearchRow, TxRow> {
        /** */
        private List<TxSearchRow> rows;

        /**
         *
         * @param major Major version.
         * @param minor Minor version.
         */
        TraversingClosure(long major, long minor) {
            super(major, minor);
        }

        /** {@inheritDoc} */
        @Override public boolean apply(BPlusTree<TxSearchRow, TxRow> tree, BPlusIO<TxSearchRow> io, long pageAddr,
            int idx) throws IgniteCheckedException {

            if (rows == null)
                rows = new ArrayList<>();

            TxLogIO logIO = (TxLogIO)io;
            int offset = io.offset(idx);

            rows.add(new TxSearchRow(logIO.getMajor(pageAddr, offset), logIO.getMinor(pageAddr, offset)));

            return true;
        }
    }
}
