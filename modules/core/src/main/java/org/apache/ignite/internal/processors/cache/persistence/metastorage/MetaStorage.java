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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IncompleteObject;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
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
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.OLD_METASTORE_PARTITION;
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

    /** This flag is used ONLY FOR TESTING the migration of a metastorage from Part 0 to Part 1. */
    public static boolean PRESERVE_LEGACY_METASTORAGE_PARTITION_ID = false;

    /** Marker for removed entry. */
    private static final byte[] TOMBSTONE = new byte[0];

    /** Temporary metastorage memory size. */
    private static final int TEMPORARY_METASTORAGE_IN_MEMORY_SIZE = 128 * 1024 * 1024;

    /** Temporary metastorage buffer size (file). */
    private static final int TEMPORARY_METASTORAGE_BUFFER_SIZE = 1024 * 1024;


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
    private final boolean readOnly;

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
    private final FailureProcessor failureProcessor;

    /** Partition id. */
    private int partId;

    /** Cctx. */
    private final GridCacheSharedContext cctx;

    /** */
    public MetaStorage(
        GridCacheSharedContext cctx,
        DataRegion dataRegion,
        DataRegionMetricsImpl regionMetrics,
        boolean readOnly
    ) {
        this.cctx = cctx;
        wal = cctx.wal();
        this.dataRegion = dataRegion;
        this.regionMetrics = regionMetrics;
        this.readOnly = readOnly;
        log = cctx.logger(getClass());
        this.failureProcessor = cctx.kernalContext().failure();
    }

    /** */
    public void init(GridCacheDatabaseSharedManager db) throws IgniteCheckedException {
        initInternal(db);

        if (!PRESERVE_LEGACY_METASTORAGE_PARTITION_ID) {
            GridCacheProcessor gcProcessor = cctx.kernalContext().cache();

            if (partId == OLD_METASTORE_PARTITION)
                gcProcessor.setTmpStorage(copyDataToTmpStorage());
            else if (gcProcessor.getTmpStorage() != null) {
                restoreDataFromTmpStorage(gcProcessor.getTmpStorage());

                gcProcessor.setTmpStorage(null);

                // remove old partitions
                CacheGroupContext cgc = cctx.cache().cacheGroup(METASTORAGE_CACHE_ID);

                if (cgc != null) {
                    db.schedulePartitionDestroy(METASTORAGE_CACHE_ID, OLD_METASTORE_PARTITION);

                    db.schedulePartitionDestroy(METASTORAGE_CACHE_ID, PageIdAllocator.INDEX_PARTITION);
                }
            }
        }
    }

    /**
     * Copying all data from the 'meta' to temporary storage.
     *
     * @return Target temporary storage
     */
    private TmpStorage copyDataToTmpStorage() throws IgniteCheckedException {
        TmpStorage tmpStorage = new TmpStorage(TEMPORARY_METASTORAGE_IN_MEMORY_SIZE, log);

        GridCursor<MetastorageDataRow> cur = tree.find(null, null);

        while (cur.next()) {
            MetastorageDataRow row = cur.get();

            tmpStorage.add(row.key(), row.value());
        }

        return tmpStorage;
    }

    /**
     * Data recovery from temporary storage
     *
     * @param tmpStorage temporary storage.
     */
    private void restoreDataFromTmpStorage(TmpStorage tmpStorage) throws IgniteCheckedException {
        for (Iterator<IgniteBiTuple<String, byte[]>> it = tmpStorage.stream().iterator(); it.hasNext(); ) {
            IgniteBiTuple<String, byte[]> t = it.next();

            putData(t.get1(), t.get2());
        }

        try {
            tmpStorage.close();
        }
        catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * @param db Database.
     */
    private void initInternal(IgniteCacheDatabaseSharedManager db) throws IgniteCheckedException {
        if (PRESERVE_LEGACY_METASTORAGE_PARTITION_ID)
            getOrAllocateMetas(partId = PageIdAllocator.OLD_METASTORE_PARTITION);
        else if (!readOnly || getOrAllocateMetas(partId = PageIdAllocator.OLD_METASTORE_PARTITION))
            getOrAllocateMetas(partId = PageIdAllocator.METASTORE_PARTITION);

        if (!empty) {
            freeList = new FreeListImpl(METASTORAGE_CACHE_ID, "metastorage",
                regionMetrics, dataRegion, null, wal, reuseListRoot.pageId().pageId(),
                reuseListRoot.isAllocated());

            MetastorageRowStore rowStore = new MetastorageRowStore(freeList, db);

            tree = new MetastorageTree(METASTORAGE_CACHE_ID, dataRegion.pageMemory(), wal, rmvId,
                freeList, rowStore, treeRoot.pageId().pageId(), treeRoot.isAllocated(), failureProcessor, partId);

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

    /**
     * Read all items from metastore.
     */
    public Collection<IgniteBiTuple<String, byte[]>> readAll() throws IgniteCheckedException {
        ArrayList<IgniteBiTuple<String, byte[]>> res = new ArrayList<>();

        GridCursor<MetastorageDataRow> cur = tree.find(null, null);

        while (cur.next()) {
            MetastorageDataRow row = cur.get();

            res.add(new IgniteBiTuple<>(row.key(), row.value()));
        }

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

    /**
     * Initializing the selected partition for use as MetaStorage
     *
     * @param partId Partition id.
     * @return true if the partion is empty
     */
    private boolean getOrAllocateMetas(int partId) throws IgniteCheckedException {
        empty = false;

        PageMemoryEx pageMem = (PageMemoryEx)dataRegion.pageMemory();

        long partMetaId = pageMem.partitionMetaPageId(METASTORAGE_CACHE_ID, partId);
        long partMetaPage = pageMem.acquirePage(METASTORAGE_CACHE_ID, partMetaId);
        try {
            if (readOnly) {
                long pageAddr = pageMem.readLock(METASTORAGE_CACHE_ID, partMetaId, partMetaPage);

                try {
                    if (PageIO.getType(pageAddr) != PageIO.T_PART_META) {
                        empty = true;

                        return true;
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

        return false;
    }

    /**
     * @return Page memory.
     */
    public PageMemory pageMemory() {
        return dataRegion.pageMemory();
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
        Executor executor = ctx.executor();

        if (executor == null) {
            freeList.saveMetadata();

            saveStoreMetadata();
        }
        else {
            executor.execute(() -> {
                try {
                    freeList.saveMetadata();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            });

            executor.execute(() -> {
                try {
                    saveStoreMetadata();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            });
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void saveStoreMetadata() throws IgniteCheckedException {
        PageMemoryEx pageMem = (PageMemoryEx) pageMemory();

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
    public class FreeListImpl extends AbstractFreeList<MetastorageDataRow> {
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

        /** {@inheritDoc} */
        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
            return pageMem.allocatePage(grpId, partId, FLAG_DATA);
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

    /**
     * Temporary storage internal
     */
    private interface TmpStorageInternal extends Closeable {
        /**
         * Put data
         *
         * @param key Key.
         * @param val Value.
         */
        boolean add(String key, byte[] val) throws IOException;

        /**
         * Read data from storage
         */
        Stream<IgniteBiTuple<String, byte[]>> stream() throws IOException;
    }

    /**
     * Temporary storage (memory)
     */
    private static class MemoryTmpStorage implements TmpStorageInternal {
        /** Buffer. */
        final ByteBuffer buf;

        /** Size. */
        int size;

        /**
         * @param size Size.
         */
        MemoryTmpStorage(int size) {
            buf = ByteBuffer.allocateDirect(size);
        }

        /** {@inheritDoc} */
        @Override public boolean add(String key, byte[] val) {
            byte[] keyData = key.getBytes(StandardCharsets.UTF_8);

            if (val.length + keyData.length + 8 > buf.remaining())
                return false;

            buf.putInt(keyData.length).putInt(val.length).put(keyData).put(val);

            size++;

            return true;
        }

        /** {@inheritDoc} */
        @Override public Stream<IgniteBiTuple<String, byte[]>> stream() {
            buf.flip();

            return Stream.generate(() -> {
                int keyLen = buf.getInt();
                int dataLen = buf.getInt();

                byte[] tmpBuf = new byte[Math.max(keyLen, dataLen)];

                buf.get(tmpBuf, 0, keyLen);

                String key = new String(tmpBuf, 0, keyLen, StandardCharsets.UTF_8);

                buf.get(tmpBuf, 0, dataLen);

                return new IgniteBiTuple<>(key, tmpBuf.length > dataLen ? Arrays.copyOf(tmpBuf, dataLen) : tmpBuf);
            }).limit(size);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
        }
    }

    /**
     * Temporary storage (file)
     */
    private static class FileTmpStorage implements TmpStorageInternal {
        /** Cache. */
        final ByteBuffer cache = ByteBuffer.allocateDirect(TEMPORARY_METASTORAGE_BUFFER_SIZE);

        /** File. */
        RandomAccessFile file;

        /** Size. */
        long size;

        /** {@inheritDoc} */
        @Override public boolean add(String key, byte[] val) throws IOException {
            if (file == null)
                file = new RandomAccessFile(File.createTempFile("m_storage", "bin"), "rw");

            byte[] keyData = key.getBytes(StandardCharsets.UTF_8);

            if (val.length + keyData.length + 8 > cache.remaining())
                flushCache(false);

            cache.putInt(keyData.length).putInt(val.length).put(keyData).put(val);

            size++;

            return true;
        }

        /** {@inheritDoc} */
        @Override public Stream<IgniteBiTuple<String, byte[]>> stream() throws IOException {
            if (file == null)
                return Stream.empty();

            flushCache(true);

            file.getChannel().position(0);

            readToCache();

            return Stream.generate(() -> {
                if (cache.remaining() <= 8) {
                    cache.compact();

                    try {
                        readToCache();
                    }
                    catch (IOException e) {
                        throw new IgniteException(e);
                    }
                }

                int keyLen = cache.getInt();
                int dataLen = cache.getInt();

                if (cache.remaining() < keyLen + dataLen) {
                    cache.compact();

                    try {
                        readToCache();
                    }
                    catch (IOException e) {
                        throw new IgniteException(e);
                    }
                }

                byte[] tmpBuf = new byte[Math.max(keyLen, dataLen)];

                cache.get(tmpBuf, 0, keyLen);

                String key = new String(tmpBuf, 0, keyLen, StandardCharsets.UTF_8);

                cache.get(tmpBuf, 0, dataLen);

                return new IgniteBiTuple<>(key, tmpBuf.length > dataLen ? Arrays.copyOf(tmpBuf, dataLen) : tmpBuf);
            }).limit(size);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            file.close();
        }

        /**
         * Read data to cache
         */
        private void readToCache() throws IOException {
            int len = (int)Math.min(file.length() - file.getChannel().position(), cache.remaining());

            while (len > 0)
                len -= file.getChannel().read(cache);

            cache.flip();
        }

        /**
         * Write cache to file.
         *
         * @param force force metadata.
         */
        private void flushCache(boolean force) throws IOException {
            if (cache.position() > 0) {
                cache.flip();

                while (cache.remaining() > 0)
                    file.getChannel().write(cache);

                cache.clear();
            }

            file.getChannel().force(force);
        }
    }

    /**
     * Temporary storage
     */
    public static class TmpStorage implements Closeable {
        /** Chain of internal storages. */
        final List<TmpStorageInternal> chain = new ArrayList<>(2);

        /** Current internal storage. */
        TmpStorageInternal current;

        /** Logger. */
        final IgniteLogger log;

        /**
         * @param memBufSize Memory buffer size.
         * @param log Logger.
         */
        TmpStorage(int memBufSize, IgniteLogger log) {
            this.log = log;

            chain.add(current = new MemoryTmpStorage(memBufSize));
        }

        /**
         * Put data
         *
         * @param key Key.
         * @param val Value.
         */
        public void add(String key, byte[] val) throws IgniteCheckedException {
            try {
                while (!current.add(key, val))
                    chain.add(current = new FileTmpStorage());
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }
        }

        /**
         * Read data from storage
         */
        public Stream<IgniteBiTuple<String, byte[]>> stream() {
            return chain.stream().flatMap(storage -> {
                try {
                    return storage.stream();
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            for (TmpStorageInternal storage : chain) {
                try {
                    storage.close();
                }
                catch (IOException ex) {
                    log.error(ex.getMessage(), ex);
                }
            }
        }
    }
}
