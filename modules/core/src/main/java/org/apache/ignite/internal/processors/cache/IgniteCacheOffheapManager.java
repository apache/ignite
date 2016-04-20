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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.database.RowStore;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdUtils.dwordsOffset;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 *
 */
public class IgniteCacheOffheapManager extends GridCacheManagerAdapter {
    /** */
    private CacheDataRowStore rowStore;

    /** */
    private CacheDataTree dataTree;

    /** */
    private final boolean enabled;

    /** */
    private final boolean indexingEnabled;

    /**
     * @param enabled Enabled flag (offheap supposed to be disabled for near cache).
     * @param indexingEnabled {@code True} if indexing is enabled for cache.
     */
    public IgniteCacheOffheapManager(boolean enabled, boolean indexingEnabled) {
        this.enabled = enabled;
        this.indexingEnabled = indexingEnabled;
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        if (enabled && !indexingEnabled) {
            IgniteCacheDatabaseSharedManager dbMgr = cctx.shared().database();

            IgniteBiTuple<FullPageId, Boolean> page = dbMgr.meta().getOrAllocateForIndex(cctx.cacheId(), cctx.namexx());

            rowStore = new CacheDataRowStore(cctx, null);

            dataTree = new CacheDataTree(rowStore, cctx, dbMgr.pageMemory(), page.get1(), page.get2());
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        super.onKernalStart0();
    }

    /**
     * @return Enabled flag.
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param part Partition.
     * @throws IgniteCheckedException If failed.
     */
    public void put(KeyCacheObject key, CacheObject val, GridCacheVersion ver, int part) throws IgniteCheckedException {
        if (!enabled || indexingEnabled)
            return;

        DataRow dataRow = new DataRow(key, val, ver, part, 0);

        rowStore.addRow(dataRow);

        dataTree.put(dataRow);
    }

    /**
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(KeyCacheObject key) throws IgniteCheckedException {
        if (!enabled || indexingEnabled)
            return;

        DataRow dataRow = dataTree.remove(new KeySearchRow(key));

        if (dataRow != null) {
            assert dataRow.link != 0 : dataRow;

            rowStore.removeRow(dataRow.link);
        }
    }

    /**
     * @param key Key to read.
     * @param part Partition.
     * @return Value tuple, if available.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgniteBiTuple<CacheObject, GridCacheVersion> read(KeyCacheObject key, int part)
        throws IgniteCheckedException {
        if (!enabled || indexingEnabled)
            return cctx.queries().read(key, part);

        DataRow dataRow = dataTree.findOne(new KeySearchRow(key));

        return dataRow != null ? F.t(dataRow.val, dataRow.ver) : null;
    }

    /**
     * Clears offheap entries.
     *
     * @param readers {@code True} to clear readers.
     */
    public void clear(boolean readers) {
        if (!enabled)
            return;

        if (dataTree != null)
            clear(dataTree, readers);

        if (indexingEnabled) {
            List<BPlusTree<?, ? extends CacheDataRow>> idxs = cctx.queries().pkIndexes();

            for (BPlusTree<?, ? extends CacheDataRow> tree : idxs)
                clear(tree, readers);
        }
    }

    /**
     * @param tree Tree.
     * @param readers {@code True} to clear readers.
     */
    private void clear(BPlusTree<?, ? extends CacheDataRow> tree, boolean readers) {
        try {
            GridCursor<? extends CacheDataRow> cur = tree.find(null, null);

            Collection<KeyCacheObject> keys = new ArrayList<>();

            while (cur.next()) {
                CacheDataRow row = cur.get();

                keys.add(row.key());
            }

            GridCacheVersion obsoleteVer = null;

            for (KeyCacheObject key : keys) {
                try {
                    if (obsoleteVer == null)
                        obsoleteVer = cctx.versions().next();

                    GridCacheEntryEx entry = cctx.cache().entryEx(key);

                    entry.clear(obsoleteVer, readers, null);
                }
                catch (GridDhtInvalidPartitionException ignore) {
                    // Ignore.
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to clear cache entry: " + key, e);
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to clear cache entries.", e);
        }
    }

    /**
     * @param tree Tree.
     * @param primary Include primary node keys.
     * @param backup Include backup node keys.
     * @param topVer Topology version.
     * @return Entries count.
     * @throws IgniteCheckedException If failed.
     */
    private long entriesCount(BPlusTree<?, ? extends CacheDataRow> tree,
        boolean primary,
        boolean backup,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
        GridCursor<? extends CacheDataRow> cur = tree.find(null, null);

        ClusterNode locNode = cctx.localNode();

        long cnt = 0;

        while (cur.next()) {
            CacheDataRow row = cur.get();

            if (primary) {
                if (cctx.affinity().primary(locNode, row.partition(), topVer)) {
                    cnt++;

                    continue;
                }
            }

            if (backup) {
                if (cctx.affinity().backup(locNode, row.partition(), topVer))
                    cnt++;
            }
        }

        return cnt;
    }

    /**
     * @param primary Include primary node keys.
     * @param backup Include backup node keys.
     * @param topVer Topology version.
     * @return Entries count.
     * @throws IgniteCheckedException If failed.
     */
    public long entriesCount(boolean primary, boolean backup, AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        if (!enabled)
            return 0;

        long cnt = 0;

        if (dataTree != null)
            cnt += entriesCount(dataTree, primary, backup, topVer);

        if (indexingEnabled) {
            List<BPlusTree<?, ? extends CacheDataRow>> idxs = cctx.queries().pkIndexes();

            for (BPlusTree<?, ? extends CacheDataRow> tree : idxs)
                cnt += entriesCount(tree, primary, backup, topVer);
        }

        return cnt;
    }

    public GridCloseableIterator<KeyCacheObject> iterator(final int part) throws IgniteCheckedException {
        if (!enabled)
            return null;

        final GridCursor<? extends CacheDataRow> cur = dataTree.find(null, null);

        return new GridCloseableIteratorAdapter<KeyCacheObject>() {
            private KeyCacheObject next;

            private void advance() throws IgniteCheckedException {
                if (next != null)
                    return;

                while (cur.next()) {
                    CacheDataRow row = cur.get();

                    if (row.partition() == part) {
                        next = (KeyCacheObject)row.key();

                        break;
                    }
                }
            }

            @Override protected KeyCacheObject onNext() throws IgniteCheckedException {
                KeyCacheObject res = next;

                next = null;

                return res;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                advance();

                return next != null;
            }
        };
    }

    /**
     *
     */
    static class KeySearchRow {
        /** */
        protected final KeyCacheObject key;

        /**
         * @param key Key.
         */
        public KeySearchRow(KeyCacheObject key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(KeySearchRow.class, this);
        }
    }

    /**
     *
     */
    static class DataRow extends KeySearchRow implements CacheDataRow {
        /** */
        private CacheObject val;

        /** */
        private GridCacheVersion ver;

        /** */
        private int part;

        /** */
        private long link;

        /**
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @param part Partition.
         * @param link Link.
         */
        public DataRow(KeyCacheObject key, CacheObject val, GridCacheVersion ver, int part, long link) {
            super(key);

            this.val = val;
            this.ver = ver;
            this.part = part;
            this.link = link;
        }

        /** {@inheritDoc} */
        @Override public KeyCacheObject key() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public CacheObject value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion version() {
            return ver;
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return part;
        }

        /** {@inheritDoc} */
        @Override public long link() {
            return link;
        }

        /** {@inheritDoc} */
        @Override public void link(long link) {
            this.link = link;
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(DataRow.class, this);
        }
    }

    /**
     *
     */
    static class CacheDataTree extends BPlusTree<KeySearchRow, DataRow> {
        /** */
        private final CacheDataRowStore rowStore;

        /** */
        private final GridCacheContext cctx;

        /**
         * @param rowStore Row store.
         * @param cctx Context.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @param initNew Initialize new index.
         * @throws IgniteCheckedException If failed.
         */
        public CacheDataTree(CacheDataRowStore rowStore,
            GridCacheContext cctx,
            PageMemory pageMem,
            FullPageId metaPageId,
            boolean initNew) throws IgniteCheckedException {
            super(cctx.cacheId(), pageMem, metaPageId);

            this.rowStore = rowStore;
            this.cctx = cctx;

            if (initNew)
                initNew();
        }

        /** {@inheritDoc} */
        @Override protected BPlusIO<KeySearchRow> io(int type, int ver) {
            if (type == PageIO.T_DATA_REF_INNER)
                return DataInnerIO.VERSIONS.forVersion(ver);

            assert type == PageIO.T_DATA_REF_LEAF: type;

            return DataLeafIO.VERSIONS.forVersion(ver);
        }

        /** {@inheritDoc} */
        @Override protected BPlusInnerIO<KeySearchRow> latestInnerIO() {
            return DataInnerIO.VERSIONS.latest();
        }

        /** {@inheritDoc} */
        @Override protected BPlusLeafIO<KeySearchRow> latestLeafIO() {
            return DataLeafIO.VERSIONS.latest();
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<KeySearchRow> io, ByteBuffer buf, int idx, KeySearchRow row) throws IgniteCheckedException {
            KeySearchRow row0 = io.getLookupRow(this, buf, idx);

            return compareKeys(row0.key, row.key);
        }

        /** {@inheritDoc} */
        @Override protected DataRow getRow(BPlusIO<KeySearchRow> io, ByteBuffer buf, int idx) throws IgniteCheckedException {
            long link = ((RowLinkIO)io).getLink(buf, idx);

            return rowStore.dataRow(link);
        }

        /**
         * @param key1 First key.
         * @param key2 Second key.
         * @return Compare result.
         * @throws IgniteCheckedException If failed.
         */
        private int compareKeys(CacheObject key1, CacheObject key2) throws IgniteCheckedException {
            byte[] bytes1 = key1.valueBytes(cctx.cacheObjectContext());
            byte[] bytes2 = key2.valueBytes(cctx.cacheObjectContext());

            int len = Math.min(bytes1.length, bytes2.length);

            for (int i = 0; i < len; i++) {
                byte b1 = bytes1[i];
                byte b2 = bytes2[i];

                if (b1 != b2)
                    return b1 > b2 ? 1 : -1;
            }

            return Integer.compare(bytes1.length, bytes2.length);
        }
    }

    /**
     *
     */
    static class CacheDataRowStore extends RowStore<DataRow> {
        /**
         * @param cctx Cache context.
         * @param freeList Free list.
         */
        public CacheDataRowStore(GridCacheContext<?, ?> cctx, FreeList freeList) {
            super(cctx, freeList);
        }

        public KeySearchRow keySearchRow(long link) throws IgniteCheckedException {
            return getRow(link, KeyRowClosure.INSTANCE);
        }

        public DataRow dataRow(long link) throws IgniteCheckedException {
            return getRow(link, DataRowClosure.INSTANCE);
        }

        private <T> T getRow(long link, RowClosure<T> c) throws IgniteCheckedException {
            try (Page page = page(pageId(link))) {
                ByteBuffer buf = page.getForRead();

                try {
                    DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

                    int dataOff = io.getDataOffset(buf, dwordsOffset(link));

                    buf.position(dataOff);

                    // Skip entry size.
                    buf.getShort();

                    T row;

                    try {
                        row = c.create(buf, link, cctx);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }

                    return row;
                }
                finally {
                    page.releaseRead();
                }
            }
        }

        /**
         *
         */
        private interface RowClosure<T> {
            T create(ByteBuffer buf, long link, GridCacheContext cctx) throws IgniteCheckedException;
        }

        /**
         *
         */
        static class KeyRowClosure implements RowClosure<KeySearchRow> {
            /** */
            static final KeyRowClosure INSTANCE = new KeyRowClosure();

            /** {@inheritDoc} */
            @Override public KeySearchRow create(ByteBuffer buf, long link, GridCacheContext cctx) throws IgniteCheckedException {
                KeyCacheObject key = cctx.cacheObjects().toKeyCacheObject(cctx.cacheObjectContext(), buf);

                return new KeySearchRow(key);
            }
        }

        /**
         *
         */
        static class DataRowClosure implements RowClosure<DataRow> {
            /** */
            static final DataRowClosure INSTANCE = new DataRowClosure();

            /** {@inheritDoc} */
            @Override public DataRow create(ByteBuffer buf, long link, GridCacheContext cctx) throws IgniteCheckedException {
                KeyCacheObject key = cctx.cacheObjects().toKeyCacheObject(cctx.cacheObjectContext(), buf);
                CacheObject val = cctx.cacheObjects().toCacheObject(cctx.cacheObjectContext(), buf);

                int topVer = buf.getInt();
                int nodeOrderDrId = buf.getInt();
                long globalTime = buf.getLong();
                long order = buf.getLong();

                GridCacheVersion ver = new GridCacheVersion(topVer, nodeOrderDrId, globalTime, order);

                return new DataRow(key, val, ver, PageIdUtils.partId(link), link);
            }
        }
    }

    /**
     *
     */
    interface RowLinkIO {
        /**
         * @param buf Buffer.
         * @param idx Index.
         * @return Row link.
         */
        public long getLink(ByteBuffer buf, int idx);

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @param link Row link.
         */
        public void setLink(ByteBuffer buf, int idx, long link);
    }

    /**
     *
     */
    static class DataInnerIO extends BPlusInnerIO<KeySearchRow> implements RowLinkIO {
        /** */
        public static final IOVersions<DataInnerIO> VERSIONS = new IOVersions<>(
            new DataInnerIO(1)
        );

        /**
         * @param ver Page format version.
         */
        private DataInnerIO(int ver) {
            super(T_DATA_REF_INNER, ver, true, 8);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer buf, int idx, KeySearchRow row) {
            DataRow row0 = (DataRow)row;

            assert row0.link != 0;

            setLink(buf, idx, row0.link);
        }

        /** {@inheritDoc} */
        @Override public KeySearchRow getLookupRow(BPlusTree<KeySearchRow,?> tree, ByteBuffer buf, int idx)
            throws IgniteCheckedException {
            long link = getLink(buf, idx);

            return ((CacheDataTree)tree).rowStore.keySearchRow(link);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<KeySearchRow> srcIo, ByteBuffer src, int srcIdx) {
            long link = ((RowLinkIO)srcIo).getLink(src, srcIdx);

            setLink(dst, dstIdx, link);
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            assert idx < getCount(buf): idx;

            return buf.getLong(offset(idx, SHIFT_LINK));
        }

        /** {@inheritDoc} */
        @Override public void setLink(ByteBuffer buf, int idx, long link) {
            buf.putLong(offset(idx, SHIFT_LINK), link);

            assert getLink(buf, idx) == link;
        }
    }

    /**
     *
     */
    static class DataLeafIO extends BPlusLeafIO<KeySearchRow> implements RowLinkIO {
        /** */
        public static final IOVersions<DataLeafIO> VERSIONS = new IOVersions<>(
            new DataLeafIO(1)
        );

        /**
         * @param ver Page format version.
         */
        protected DataLeafIO(int ver) {
            super(T_DATA_REF_LEAF, ver, 8);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer buf, int idx, KeySearchRow row) {
            DataRow row0 = (DataRow)row;

            assert row0.link != 0;

            setLink(buf, idx, row0.link);
        }

        /** {@inheritDoc} */
        @Override public KeySearchRow getLookupRow(BPlusTree<KeySearchRow,?> tree, ByteBuffer buf, int idx)
            throws IgniteCheckedException {
            long link = getLink(buf, idx);

            return ((CacheDataTree)tree).rowStore.keySearchRow(link);
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            assert idx < getCount(buf): idx;

            return buf.getLong(offset(idx));
        }

        /** {@inheritDoc} */
        @Override public void setLink(ByteBuffer buf, int idx, long link) {
            buf.putLong(offset(idx), link);

            assert getLink(buf, idx) == link;
        }
    }
}
