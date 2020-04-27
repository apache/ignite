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

package org.apache.ignite.internal.processors.cache.tree;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccCacheIdAwareDataInnerIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccCacheIdAwareDataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataInnerIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccDataPageClosure;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.TestOnly;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.MVCC_INFO_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.T_DATA;
import static org.apache.ignite.internal.util.GridArrays.clearTail;

/**
 *
 */
public class CacheDataTree extends BPlusTree<CacheSearchRow, CacheDataRow> {
    /** */
    private static final CacheDataRow[] EMPTY_ROWS = {};

    /** */
    private static Boolean lastFindWithDataPageScan;

    /** */
    private static final ThreadLocal<Boolean> dataPageScanEnabled =
        ThreadLocal.withInitial(() -> false);

    /** */
    private final CacheDataRowStore rowStore;

    /** */
    private final CacheGroupContext grp;

    /**
     * @param grp Cache group.
     * @param name Tree name.
     * @param reuseList Reuse list.
     * @param rowStore Row store.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public CacheDataTree(
        CacheGroupContext grp,
        String name,
        ReuseList reuseList,
        CacheDataRowStore rowStore,
        long metaPageId,
        boolean initNew,
        PageLockListener lockLsnr
    ) throws IgniteCheckedException {
        super(
            name,
            grp.groupId(),
            grp.name(),
            grp.dataRegion().pageMemory(),
            grp.dataRegion().config().isPersistenceEnabled() ? grp.shared().wal() : null,
            grp.offheap().globalRemoveId(),
            metaPageId,
            reuseList,
            innerIO(grp),
            leafIO(grp),
            grp.shared().kernalContext().failure(),
            lockLsnr
        );

        assert rowStore != null;

        this.rowStore = rowStore;
        this.grp = grp;

        assert !grp.dataRegion().config().isPersistenceEnabled() || grp.shared().database().checkpointLockIsHeldByThread();

        initTree(initNew);
    }

    /**
     * Enable or disable data page scan.
     * @param enabled {code true} If enabled.
     */
    public static void setDataPageScanEnabled(boolean enabled) {
        dataPageScanEnabled.set(enabled);
    }

    /**
     * @return {@code true} If data page scan is enabled.
     */
    public static boolean isDataPageScanEnabled() {
        return dataPageScanEnabled.get();
    }

    /**
     * @return {@code true} If the last observed call to the method {@code find(...)} used data page scan.
     */
    @TestOnly
    public static Boolean isLastFindWithDataPageScan() {
        Boolean res = lastFindWithDataPageScan;
        lastFindWithDataPageScan = null;
        return res;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<CacheDataRow> find(
        CacheSearchRow lower,
        CacheSearchRow upper,
        TreeRowClosure<CacheSearchRow,CacheDataRow> c,
        Object x
    ) throws IgniteCheckedException {
        // If there is a group of caches, lower and upper bounds will not be null here.
        if (lower == null
                && upper == null
                && grp.persistenceEnabled()
                && dataPageScanEnabled.get()
                && (c == null || c instanceof MvccDataPageClosure))
            return scanDataPages(asRowData(x), (MvccDataPageClosure)c);

        lastFindWithDataPageScan = FALSE;
        return super.find(lower, upper, c, x);
    }

    /**
     * @param rowData Required row data.
     * @param c Optional MVCC closure.
     * @return Cache row cursor.
     * @throws IgniteCheckedException If failed.
     */
    private GridCursor<CacheDataRow> scanDataPages(CacheDataRowAdapter.RowData rowData, MvccDataPageClosure c)
        throws IgniteCheckedException {
        lastFindWithDataPageScan = TRUE;

        checkDestroyed();

        assert rowData != null;
        assert grp.persistenceEnabled();

        int partId = rowStore.getPartitionId();
        GridCacheSharedContext shared = grp.shared();
        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)shared.database();
        PageStore pageStore = db.getPageStore(grpId, partId);
        boolean mvccEnabled = grp.mvccEnabled();
        int pageSize = pageSize();

        long startPageId = ((PageMemoryEx)pageMem).partitionMetaPageId(grp.groupId(), partId);

        final class DataPageScanCursor implements GridCursor<CacheDataRow> {
            /** */
            int pagesCnt = pageStore.pages();

            /** */
            int curPage = -1;

            /** */
            CacheDataRow[] rows = EMPTY_ROWS;

            /** */
            int curRow = -1;

            /** {@inheritDoc} */
            @Override public boolean next() throws IgniteCheckedException {
                if (rows == null)
                    return false;

                if (++curRow < rows.length && rows[curRow] != null)
                    return true;

                return readNextDataPage();
            }

            /**
             * @return {@code true} If new rows were fetched.
             * @throws IgniteCheckedException If failed.
             */
            private boolean readNextDataPage() throws IgniteCheckedException {
                checkDestroyed();

                for (;;) {
                    if (++curPage >= pagesCnt) {
                        // Reread number of pages when we reach it (it may grow).
                        int newPagesCnt = pageStore.pages();

                        if (newPagesCnt <= pagesCnt) {
                            rows = null;
                            return false;
                        }

                        pagesCnt = newPagesCnt;
                    }

                    long pageId = startPageId + curPage;
                    long page = pageMem.acquirePage(grpId, pageId);

                    try {
                        boolean skipVer = CacheDataRowStore.getSkipVersion();

                        long pageAddr = ((PageMemoryEx)pageMem).readLock(page, pageId, true, false);

                        try {
                            // TODO https://issues.apache.org/jira/browse/IGNITE-11998.
                            // Here we should also exclude fragmented pages that don't contain the head of the entry.
                            if (PageIO.getType(pageAddr) != T_DATA)
                                continue; // Not a data page.

                            DataPageIO io = PageIO.getPageIO(T_DATA, PageIO.getVersion(pageAddr));

                            int rowsCnt = io.getRowsCount(pageAddr);

                            if (rowsCnt == 0)
                                continue; // Empty page.

                            if (rowsCnt > rows.length)
                                rows = new CacheDataRow[rowsCnt];
                            else
                                clearTail(rows, rowsCnt);

                            int r = 0;

                            for (int i = 0; i < rowsCnt; i++) {
                                if (c == null || c.applyMvcc(io, pageAddr, i, pageSize)) {
                                    DataRow row = mvccEnabled ? new MvccDataRow() : new DataRow();

                                    row.initFromDataPage(
                                        io,
                                        pageAddr,
                                        i,
                                        grp,
                                        shared,
                                        pageMem,
                                        rowData,
                                        skipVer
                                    );

                                    rows[r++] = row;
                                }
                            }

                            if (r == 0)
                                continue; // No rows fetched in this page.

                            curRow = 0;
                            return true;
                        }
                        finally {
                            pageMem.readUnlock(grpId, pageId, page);
                        }
                    }
                    finally {
                        pageMem.releasePage(grpId, pageId, page);
                    }
                }
            }

            /** {@inheritDoc} */
            @Override public CacheDataRow get() {
                return rows[curRow];
            }
        }

        return new DataPageScanCursor();
    }

    /**
     * @param flags Flags.
     * @return Row data.
     */
    private static CacheDataRowAdapter.RowData asRowData(Object flags) {
        return flags != null ? (CacheDataRowAdapter.RowData)flags :
            CacheDataRowAdapter.RowData.FULL;
    }

    /**
     * @param grp Cache group.
     * @return Tree inner IO.
     */
    private static IOVersions<? extends AbstractDataInnerIO> innerIO(CacheGroupContext grp) {
        if (grp.mvccEnabled())
            return grp.sharedGroup() ? MvccCacheIdAwareDataInnerIO.VERSIONS : MvccDataInnerIO.VERSIONS;

        return grp.sharedGroup() ? CacheIdAwareDataInnerIO.VERSIONS : DataInnerIO.VERSIONS;
    }

    /**
     * @param grp Cache group.
     * @return Tree leaf IO.
     */
    private static IOVersions<? extends AbstractDataLeafIO> leafIO(CacheGroupContext grp) {
        if (grp.mvccEnabled())
            return grp.sharedGroup() ? MvccCacheIdAwareDataLeafIO.VERSIONS : MvccDataLeafIO.VERSIONS;

        return grp.sharedGroup() ? CacheIdAwareDataLeafIO.VERSIONS : DataLeafIO.VERSIONS;
    }

    /**
     * @return Row store.
     */
    public CacheDataRowStore rowStore() {
        return rowStore;
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<CacheSearchRow> iox, long pageAddr, int idx, CacheSearchRow row)
        throws IgniteCheckedException {
        assert !grp.mvccEnabled() || row.mvccCoordinatorVersion() != MvccUtils.MVCC_CRD_COUNTER_NA
            || (row.getClass() == SearchRow.class && row.key() == null) : row;

        RowLinkIO io = (RowLinkIO)iox;

        int cmp;

        if (grp.sharedGroup()) {
            assert row.cacheId() != CU.UNDEFINED_CACHE_ID : "Cache ID is not provided: " + row;

            int cacheId = io.getCacheId(pageAddr, idx);

            cmp = Integer.compare(cacheId, row.cacheId());

            if (cmp != 0)
                return cmp;

            if (row.key() == null) {
                assert row.getClass() == SearchRow.class : row;

                // A search row with a cache ID only is used as a cache bound.
                // The found position will be shifted until the exact cache bound is found;
                // See for details:
                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findLowerBound()
                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findUpperBound()
                return cmp;
            }
        }

        cmp = Integer.compare(io.getHash(pageAddr, idx), row.hash());

        if (cmp != 0)
            return cmp;

        long link = io.getLink(pageAddr, idx);

        assert row.key() != null : row;

        cmp = compareKeys(row.key(), link);

        if (cmp != 0 || !grp.mvccEnabled())
            return cmp;

        long crd = io.getMvccCoordinatorVersion(pageAddr, idx);
        long cntr = io.getMvccCounter(pageAddr, idx);
        int opCntr = io.getMvccOperationCounter(pageAddr, idx);

        assert MvccUtils.mvccVersionIsValid(crd, cntr, opCntr);

        return -MvccUtils.compare(crd, cntr, opCntr, row); // descending order
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow getRow(BPlusIO<CacheSearchRow> io, long pageAddr, int idx, Object flags) {
        RowLinkIO rowIo = (RowLinkIO)io;

        long link = rowIo.getLink(pageAddr, idx);
        int hash = rowIo.getHash(pageAddr, idx);

        int cacheId = grp.sharedGroup() ? rowIo.getCacheId(pageAddr, idx) : CU.UNDEFINED_CACHE_ID;

        CacheDataRowAdapter.RowData x = asRowData(flags);

        if (grp.mvccEnabled()) {
            long mvccCrdVer = rowIo.getMvccCoordinatorVersion(pageAddr, idx);
            long mvccCntr = rowIo.getMvccCounter(pageAddr, idx);
            int mvccOpCntr = rowIo.getMvccOperationCounter(pageAddr, idx);

            return rowStore.mvccRow(cacheId, hash, link, x, mvccCrdVer, mvccCntr, mvccOpCntr);
        }
        else
            return rowStore.dataRow(cacheId, hash, link, x);
    }

    /** {@inheritDoc} */
    @Override protected IoStatisticsHolder statisticsHolder() {
        return grp.statisticsHolderIdx();
    }

    /**
     * @param key Key.
     * @param link Link.
     * @return Compare result.
     * @throws IgniteCheckedException If failed.
     */
    private int compareKeys(KeyCacheObject key, final long link) throws IgniteCheckedException {
        byte[] bytes = key.valueBytes(grp.cacheObjectContext());

        final long pageId = pageId(link);
        final long page = acquirePage(pageId);

        try {
            long pageAddr = readLock(pageId, page); // Non-empty data page must not be recycled.

            assert pageAddr != 0L : link;

            try {
                DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

                DataPagePayload data = io.readPayload(pageAddr,
                    itemId(link),
                    pageSize());

                if (data.nextLink() == 0) {
                    long addr = pageAddr + data.offset();

                    if (grp.mvccEnabled())
                        addr += MVCC_INFO_SIZE; // Skip MVCC info.

                    if (grp.storeCacheIdInDataPage())
                        addr += 4; // Skip cache id.

                    final int len = PageUtils.getInt(addr, 0);

                    int lenCmp = Integer.compare(len, bytes.length);

                    if (lenCmp != 0)
                        return lenCmp;

                    addr += 5; // Skip length and type byte.

                    final int words = len / 8;

                    for (int i = 0; i < words; i++) {
                        int off = i * 8;

                        long b1 = PageUtils.getLong(addr, off);
                        long b2 = GridUnsafe.getLong(bytes, GridUnsafe.BYTE_ARR_OFF + off);

                        int cmp = Long.compare(b1, b2);

                        if (cmp != 0)
                            return cmp;
                    }

                    for (int i = words * 8; i < len; i++) {
                        byte b1 = PageUtils.getByte(addr, i);
                        byte b2 = bytes[i];

                        if (b1 != b2)
                            return b1 > b2 ? 1 : -1;
                    }

                    return 0;
                }
            }
            finally {
                readUnlock(pageId, page, pageAddr);
            }
        }
        finally {
            releasePage(pageId, page);
        }

        // TODO GG-11768.
        CacheDataRowAdapter other = grp.mvccEnabled() ? new MvccDataRow(link) : new CacheDataRowAdapter(link);
        other.initFromLink(grp, CacheDataRowAdapter.RowData.KEY_ONLY);

        byte[] bytes1 = other.key().valueBytes(grp.cacheObjectContext());
        byte[] bytes2 = key.valueBytes(grp.cacheObjectContext());

        int lenCmp = Integer.compare(bytes1.length, bytes2.length);

        if (lenCmp != 0)
            return lenCmp;

        final int len = bytes1.length;
        final int words = len / 8;

        for (int i = 0; i < words; i++) {
            int off = GridUnsafe.BYTE_ARR_INT_OFF + i * 8;

            long b1 = GridUnsafe.getLong(bytes1, off);
            long b2 = GridUnsafe.getLong(bytes2, off);

            int cmp = Long.compare(b1, b2);

            if (cmp != 0)
                return cmp;
        }

        for (int i = words * 8; i < len; i++) {
            byte b1 = bytes1[i];
            byte b2 = bytes2[i];

            if (b1 != b2)
                return b1 > b2 ? 1 : -1;
        }

        return 0;
    }
}
