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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IncompleteObject;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 * General purpose key-value local-only storage.
 */
public class MetaStorage implements DbCheckpointListener, ReadOnlyMetastorage, ReadWriteMetastorage {
    /** */
    public static final String METASTORAGE_CACHE_NAME = "MetaStorage";

    /** */
    public static final int METASTORAGE_CACHE_ID = CU.cacheId(METASTORAGE_CACHE_NAME);

    /** Marker for removed entry. */
    private static final byte[] TOMBSTONE = new byte[0];

    /** */
    private final IgniteWriteAheadLogManager wal;

    /** */
    private final DataRegion dataRegion;

    /** */
    private final IgniteLogger log;

    /** */
    private MetastorageTree tree;

    /** */
    private AtomicLong rmvId = new AtomicLong();

    /** */
    private DataRegionMetricsImpl regionMetrics;

    /** */
    private boolean readOnly;

    /** */
    private boolean empty;

    /** */
    private RootPage treeRoot;

    /** */
    private RootPage reuseListRoot;

    /** */
    private FreeListImpl freeList;

    /** */
    private Map<String, byte[]> lastUpdates;

    /** */
    private final Marshaller marshaller = new JdkMarshaller();

    /** */
    public MetaStorage(
        GridCacheSharedContext cctx,
        DataRegion dataRegion,
        DataRegionMetricsImpl regionMetrics,
        boolean readOnly
    ) {
        wal = cctx.wal();
        this.dataRegion = dataRegion;
        this.regionMetrics = regionMetrics;
        this.readOnly = readOnly;
        log = cctx.logger(getClass());
    }

    /** */
    public MetaStorage(GridCacheSharedContext cctx, DataRegion memPlc, DataRegionMetricsImpl memMetrics) {
        this(cctx, memPlc, memMetrics, false);
    }

    /** */
    public void init(IgniteCacheDatabaseSharedManager db) throws IgniteCheckedException {
        getOrAllocateMetas();

        if (!empty) {
            freeList = new FreeListImpl(METASTORAGE_CACHE_ID, "metastorage",
                regionMetrics, dataRegion, null, wal, reuseListRoot.pageId().pageId(),
                reuseListRoot.isAllocated());

            MetastorageRowStore rowStore = new MetastorageRowStore(freeList, db);

            tree = new MetastorageTree(METASTORAGE_CACHE_ID, dataRegion.pageMemory(), wal, rmvId,
                freeList, rowStore, treeRoot.pageId().pageId(), treeRoot.isAllocated());

            if (!readOnly)
                ((GridCacheDatabaseSharedManager)db).addCheckpointListener(this);
        }
    }

    /** {@inheritDoc} */
    @Override public Serializable read(String key) throws IgniteCheckedException {
        byte[] data = getData(key);

        Object result = null;

        if (data != null)
            result = marshaller.unmarshal(data, getClass().getClassLoader());

        return (Serializable)result;
    }

    /** {@inheritDoc} */
    @Override public Map<String, ? extends Serializable> readForPredicate(IgnitePredicate<String> keyPred)
        throws IgniteCheckedException {
        Map<String, Serializable> res = null;

        if (readOnly) {
            if (empty)
                return Collections.emptyMap();

            if (lastUpdates != null) {
                for (Map.Entry<String, byte[]> lastUpdate : lastUpdates.entrySet()) {
                    if (keyPred.apply(lastUpdate.getKey())) {
                        byte[] valBytes = lastUpdate.getValue();

                        if (valBytes == TOMBSTONE)
                            continue;

                        if (res == null)
                            res = new HashMap<>();

                        Serializable val = marshaller.unmarshal(valBytes, getClass().getClassLoader());

                        res.put(lastUpdate.getKey(), val);
                    }
                }
            }
        }

        GridCursor<MetastorageDataRow> cur = tree.find(null, null);

        while (cur.next()) {
            MetastorageDataRow row = cur.get();

            String key = row.key();
            byte[] valBytes = row.value();

            if (keyPred.apply(key)) {
                // Either already added it, or this is a tombstone -> ignore.
                if (lastUpdates != null && lastUpdates.containsKey(key))
                    continue;

                if (res == null)
                    res = new HashMap<>();

                Serializable val = marshaller.unmarshal(valBytes, getClass().getClassLoader());

                res.put(key, val);
            }
        }

        if (res == null)
            res = Collections.emptyMap();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void write(@NotNull String key, @NotNull Serializable val) throws IgniteCheckedException {
        assert val != null;

        byte[] data = marshaller.marshal(val);

        putData(key, data);
    }

    /** {@inheritDoc} */
    @Override public void remove(@NotNull String key) throws IgniteCheckedException {
        removeData(key);
    }

    /** */
    public void putData(String key, byte[] data) throws IgniteCheckedException {
        if (!readOnly) {
            WALPointer ptr = wal.log(new MetastoreDataRecord(key, data));

            wal.flush(ptr, false);

            synchronized (this) {
                MetastorageDataRow oldRow = tree.findOne(new MetastorageDataRow(key, null));

                if (oldRow != null) {
                    tree.removex(oldRow);
                    tree.rowStore().removeRow(oldRow.link());
                }

                MetastorageDataRow row = new MetastorageDataRow(key, data);
                tree.rowStore().addRow(row);
                tree.put(row);
            }
        }
    }

    /** */
    public byte[] getData(String key) throws IgniteCheckedException {
        if (readOnly) {
            if (lastUpdates != null) {
                byte[] res = lastUpdates.get(key);

                if (res != null)
                    return res != TOMBSTONE ? res : null;
            }

            if (empty)
                return null;
        }

        MetastorageDataRow row = tree.findOne(new MetastorageDataRow(key, null));

        if (row == null)
            return null;

        return row.value();
    }

    /** */
    public void removeData(String key) throws IgniteCheckedException {
        if (!readOnly) {
            WALPointer ptr = wal.log(new MetastoreDataRecord(key, null));

            wal.flush(ptr, false);

            synchronized (this) {
                MetastorageDataRow row = new MetastorageDataRow(key, null);
                MetastorageDataRow oldRow = tree.findOne(row);

                if (oldRow != null) {
                    tree.removex(oldRow);
                    tree.rowStore().removeRow(oldRow.link());
                }
            }
        }
    }

    /** */
    private void checkRootsPageIdFlag(long treeRoot, long reuseListRoot) throws StorageException {
        if (PageIdUtils.flag(treeRoot) != PageMemory.FLAG_DATA)
            throw new StorageException("Wrong tree root page id flag: treeRoot="
                + U.hexLong(treeRoot) + ", METASTORAGE_CACHE_ID=" + METASTORAGE_CACHE_ID);

        if (PageIdUtils.flag(reuseListRoot) != PageMemory.FLAG_DATA)
            throw new StorageException("Wrong reuse list root page id flag: reuseListRoot="
                + U.hexLong(reuseListRoot) + ", METASTORAGE_CACHE_ID=" + METASTORAGE_CACHE_ID);
    }

    /** */
    private void getOrAllocateMetas() throws IgniteCheckedException {
        PageMemoryEx pageMem = (PageMemoryEx)dataRegion.pageMemory();

        int partId = 0;

        long partMetaId = pageMem.partitionMetaPageId(METASTORAGE_CACHE_ID, partId);
        long partMetaPage = pageMem.acquirePage(METASTORAGE_CACHE_ID, partMetaId);
        try {
            if (readOnly) {
                long pageAddr = pageMem.readLock(METASTORAGE_CACHE_ID, partMetaId, partMetaPage);

                try {
                    if (PageIO.getType(pageAddr) != PageIO.T_PART_META) {
                        empty = true;

                        return;
                    }

                    PagePartitionMetaIO io = PageIO.getPageIO(pageAddr);

                    long treeRoot = io.getTreeRoot(pageAddr);
                    long reuseListRoot = io.getReuseListRoot(pageAddr);

                    checkRootsPageIdFlag(treeRoot, reuseListRoot);

                    this.treeRoot = new RootPage(new FullPageId(treeRoot, METASTORAGE_CACHE_ID), false);
                    this.reuseListRoot = new RootPage(new FullPageId(reuseListRoot, METASTORAGE_CACHE_ID), false);

                    rmvId.set(io.getGlobalRemoveId(pageAddr));
                }
                finally {
                    pageMem.readUnlock(METASTORAGE_CACHE_ID, partId, partMetaPage);
                }
            }
            else {
                boolean allocated = false;
                long pageAddr = pageMem.writeLock(METASTORAGE_CACHE_ID, partMetaId, partMetaPage);

                try {
                    long treeRoot, reuseListRoot;

                    if (PageIO.getType(pageAddr) != PageIO.T_PART_META) {
                        // Initialize new page.
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

                        rmvId.set(io.getGlobalRemoveId(pageAddr));

                        checkRootsPageIdFlag(treeRoot, reuseListRoot);
                    }

                    this.treeRoot = new RootPage(new FullPageId(treeRoot, METASTORAGE_CACHE_ID), allocated);
                    this.reuseListRoot = new RootPage(new FullPageId(reuseListRoot, METASTORAGE_CACHE_ID), allocated);
                }
                finally {
                    pageMem.writeUnlock(METASTORAGE_CACHE_ID, partMetaId, partMetaPage, null, allocated);
                }
            }
        }
        finally {
            pageMem.releasePage(METASTORAGE_CACHE_ID, partMetaId, partMetaPage);
        }
    }

    /**
     * @return Page memory.
     */
    public PageMemory pageMemory() {
        return dataRegion.pageMemory();
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
        freeList.saveMetadata();

        saveStoreMetadata();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void saveStoreMetadata() throws IgniteCheckedException {
        PageMemoryEx pageMem = (PageMemoryEx) pageMemory();

        int partId = 0;

        long partMetaId = pageMem.partitionMetaPageId(METASTORAGE_CACHE_ID, partId);
        long partMetaPage = pageMem.acquirePage(METASTORAGE_CACHE_ID, partMetaId);

        try {
            long partMetaPageAddr = pageMem.writeLock(METASTORAGE_CACHE_ID, partMetaId, partMetaPage);

            if (partMetaPageAddr == 0L) {
                U.warn(log, "Failed to acquire write lock for meta page [metaPage=" + partMetaPage + ']');

                return;
            }

            boolean changed = false;

            try {
                PagePartitionMetaIO io = PageIO.getPageIO(partMetaPageAddr);

                changed |= io.setGlobalRemoveId(partMetaPageAddr, rmvId.get());
            }
            finally {
                pageMem.writeUnlock(METASTORAGE_CACHE_ID, partMetaId, partMetaPage, null, changed);
            }
        }
        finally {
            pageMem.releasePage(METASTORAGE_CACHE_ID, partMetaId, partMetaPage);
        }
    }

    /** */
    public void applyUpdate(String key, byte[] value) throws IgniteCheckedException {
        if (readOnly) {
            if (lastUpdates == null)
                lastUpdates = new HashMap<>();

            lastUpdates.put(key, value != null ? value : TOMBSTONE);
        }
        else {
            if (value != null)
                putData(key, value);
            else
                removeData(key);
        }
    }

    /** */
    public static class FreeListImpl extends AbstractFreeList<MetastorageDataRow> {
        /** {@inheritDoc} */
        FreeListImpl(int cacheId, String name, DataRegionMetricsImpl regionMetrics, DataRegion dataRegion,
            ReuseList reuseList,
            IgniteWriteAheadLogManager wal, long metaPageId, boolean initNew) throws IgniteCheckedException {
            super(cacheId, name, regionMetrics, dataRegion, reuseList, wal, metaPageId, initNew);
        }

        /** {@inheritDoc} */
        @Override public IOVersions<? extends AbstractDataPageIO<MetastorageDataRow>> ioVersions() {
            return SimpleDataPageIO.VERSIONS;
        }

        /**
         * Read row from data pages.
         */
        final MetastorageDataRow readRow(String key, long link)
            throws IgniteCheckedException {
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
                        SimpleDataPageIO io = (SimpleDataPageIO)ioVersions().forPage(pageAddr);

                        DataPagePayload data = io.readPayload(pageAddr, itemId(nextLink), pageMem.pageSize());

                        nextLink = data.nextLink();

                        if (first) {
                            if (nextLink == 0) {
                                // Fast path for a single page row.
                                return new MetastorageDataRow(link, key, SimpleDataPageIO.readPayload(pageAddr + data.offset()));
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

            return new MetastorageDataRow(link, key, incomplete.data());
        }
    }
}
