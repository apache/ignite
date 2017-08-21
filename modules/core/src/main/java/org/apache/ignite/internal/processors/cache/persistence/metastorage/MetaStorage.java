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
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRecord;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.MemoryMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.MemoryPolicy;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class MetaStorage {
    /** */
    public static final int METASTORAGE_CACHE_ID = CU.cacheId("MetaStorage");

    /** */
    private final IgniteWriteAheadLogManager wal;
    /** */
    private final MemoryPolicy memPlc;
    /** */
    private MetastorageTree tree;
    /** */
    private AtomicLong rmvId = new AtomicLong();
    /** */
    private MemoryMetricsImpl memMetrics;
    /** */
    private boolean readOnly;

    private RootPage treeRoot;

    private RootPage reuseListRoot;

    /** */
    public MetaStorage(IgniteWriteAheadLogManager wal, MemoryPolicy memPlc, MemoryMetricsImpl memMetrics,
        boolean readOnly) {
        this.wal = wal;
        this.memPlc = memPlc;
        this.memMetrics = memMetrics;
        this.readOnly = readOnly;
    }

    /** */
    public MetaStorage(IgniteWriteAheadLogManager wal, MemoryPolicy memPlc, MemoryMetricsImpl memMetrics) {
        this(wal, memPlc, memMetrics, false);
    }

    /** */
    public void start(IgniteCacheDatabaseSharedManager db) throws IgniteCheckedException {
        memPlc.pageMemory().start();
//        memPlc.evictionTracker().start();

        getOrAllocateMetas();

        FreeListImpl freeList = new FreeListImpl(METASTORAGE_CACHE_ID, "metastorage",
            memMetrics, memPlc, null, wal, reuseListRoot.pageId().pageId(),
            reuseListRoot.isAllocated());

        MetastorageRowStore rowStore = new MetastorageRowStore(freeList, db);

        tree = new MetastorageTree(METASTORAGE_CACHE_ID, memPlc.pageMemory(), wal, rmvId,
            freeList, rowStore, treeRoot.pageId().pageId(), treeRoot.isAllocated());
    }

    /** */
    public void putData(String key, byte[] data) throws IgniteCheckedException {
        if (!readOnly) {
            synchronized (this) {
                MetastorageDataRow row = new MetastorageDataRow(key, data);
                MetastorageDataRow oldRow = tree.findOne(row);

                if (oldRow != null) {
                    tree.rowStore().removeRow(oldRow.link());
                    tree.remove(oldRow);
                }
                else {
                    tree.rowStore().addRow(row);
                    tree.put(row);
                }
            }
        }
    }

    /** */
    public MetastorageDataRow getData(String key) throws IgniteCheckedException {
        MetastorageDataRow row = tree.findOne(new MetastorageDataRow(key, null));
        return row;
    }

    /** */
    public void removeData(String key) throws IgniteCheckedException {
        if (!readOnly)
            tree.remove(new MetastorageDataRow(key, null));
    }

    /**
     */
    private void getOrAllocateMetas() throws IgniteCheckedException {
        PageMemoryEx pageMem = (PageMemoryEx)memPlc.pageMemory();

        int partId = 0;

        long partMetaId = pageMem.partitionMetaPageId(METASTORAGE_CACHE_ID, partId);
        long partMetaPage = pageMem.acquirePage(METASTORAGE_CACHE_ID, partMetaId);
        try {
            boolean allocated = false;
            long pageAddr = pageMem.writeLock(METASTORAGE_CACHE_ID, partMetaId, partMetaPage);

            try {
                long treeRoot, reuseListRoot;

                // Initialize new page.
                if (PageIO.getType(pageAddr) != PageIO.T_PART_META) {
                    if (readOnly)
                        throw new IgniteCheckedException("metastorage is not initialized");

                    PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.latest();

                    io.initNewPage(pageAddr, partMetaId, pageMem.pageSize());

                    treeRoot = pageMem.allocatePage(METASTORAGE_CACHE_ID, partId, PageMemory.FLAG_DATA);
                    reuseListRoot = pageMem.allocatePage(METASTORAGE_CACHE_ID, partId, PageMemory.FLAG_DATA);

                    assert PageIdUtils.flag(treeRoot) == PageMemory.FLAG_DATA;
                    assert PageIdUtils.flag(reuseListRoot) == PageMemory.FLAG_DATA;

                    io.setTreeRoot(pageAddr, treeRoot);
                    io.setReuseListRoot(pageAddr, reuseListRoot);

                    if (PageHandler.isWalDeltaRecordNeeded(pageMem, METASTORAGE_CACHE_ID, partMetaId, partMetaPage, wal, null))
                        wal.log(new MetaPageInitRecord(
                            METASTORAGE_CACHE_ID,
                            partMetaId,
                            io.getType(),
                            io.getVersion(),
                            treeRoot,
                            reuseListRoot
                        ));

                    allocated = true;
                }
                else {
                    PagePartitionMetaIO io = PageIO.getPageIO(pageAddr);

                    treeRoot = io.getTreeRoot(pageAddr);
                    reuseListRoot = io.getReuseListRoot(pageAddr);

                    assert PageIdUtils.flag(treeRoot) == PageMemory.FLAG_DATA :
                        U.hexLong(treeRoot) + ", part=" + partId + ", METASTORAGE_CACHE_ID=" + METASTORAGE_CACHE_ID;
                    assert PageIdUtils.flag(reuseListRoot) == PageMemory.FLAG_DATA :
                        U.hexLong(reuseListRoot) + ", part=" + partId + ", METASTORAGE_CACHE_ID=" + METASTORAGE_CACHE_ID;
                }

                this.treeRoot = new RootPage(new FullPageId(treeRoot, METASTORAGE_CACHE_ID), allocated);
                this.reuseListRoot = new RootPage(new FullPageId(reuseListRoot, METASTORAGE_CACHE_ID), allocated);
            }
            finally {
                pageMem.writeUnlock(METASTORAGE_CACHE_ID, partMetaId, partMetaPage, null, allocated);
            }
        }
        finally {
            pageMem.releasePage(METASTORAGE_CACHE_ID, partMetaId, partMetaPage);
        }
    }

    /** */
    private static class FreeListImpl extends AbstractFreeList<MetastorageDataRow> {
        /** {@inheritDoc} */
        public FreeListImpl(int cacheId, String name, MemoryMetricsImpl memMetrics, MemoryPolicy memPlc,
            ReuseList reuseList,
            IgniteWriteAheadLogManager wal, long metaPageId, boolean initNew) throws IgniteCheckedException {
            super(cacheId, name, memMetrics, memPlc, reuseList, wal, metaPageId, initNew);
        }

        /** {@inheritDoc} */
        @Override
        public IOVersions<? extends AbstractDataPageIO<MetastorageDataRow>> ioVersions() {
            return SimpleDataPageIO.VERSIONS;
        }
    }
}
