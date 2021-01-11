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

package org.apache.ignite.internal.cache.query.index.sorted.defragmentation;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexSchema;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineRecommender;
import org.apache.ignite.internal.cache.query.index.sorted.inline.MetaPageInfo;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineLeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineLeafIO;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;

/**
 * Creates temporary index to defragment old index.
 */
public class DefragIndexFactory extends InlineIndexFactory {
    /** Temporary offheap manager. */
    private final IgniteCacheOffheapManager offheap;

    /** Old index. */
    private final InlineIndex oldIdx;

    /** Temporary cache page memory. */
    private final PageMemory newCachePageMemory;

    /** */
    public DefragIndexFactory(IgniteCacheOffheapManager offheap, PageMemory newCachePageMemory, InlineIndex oldIdx) {
        this.offheap = offheap;
        this.oldIdx = oldIdx;
        this.newCachePageMemory = newCachePageMemory;
    }

    /** {@inheritDoc} */
    @Override protected InlineIndexTree createIndexSegment(GridCacheContext<?, ?> cctx, SortedIndexDefinition def,
        RootPage rootPage, IoStatisticsHolder stats, InlineRecommender recommender, int segmentNum) throws Exception {

        InlineIndexTree tree = new InlineIndexTree(
            def,
            cctx,
            def.getTreeName(),
            offheap,
            offheap.reuseListForIndex(def.getTreeName()),
            newCachePageMemory,
            getPageIoResolver(def),
            rootPage.pageId().pageId(),
            rootPage.isAllocated(),
            oldIdx.inlineSize(),
            null,
            null
        );

        final MetaPageInfo oldInfo = oldIdx.getSegment(segmentNum).getMetaInfo();

        tree.copyMetaInfo(oldInfo);

        tree.enableSequentialWriteMode();

        return tree;
    }

    /** {@inheritDoc} */
    @Override protected RootPage getRootPage(GridCacheContext<?, ?> ctx, String treeName, int segment) throws Exception {
        return offheap.rootPageForIndex(ctx.cacheId(), treeName, segment);
    }

    /** */
    private PageIoResolver getPageIoResolver(SortedIndexDefinition def) {
        return pageAddr -> {
            PageIO io = PageIoResolver.DEFAULT_PAGE_IO_RESOLVER.resolve(pageAddr);

            if (io instanceof BPlusMetaIO)
                return io;

            //noinspection unchecked,rawtypes,rawtypes
            return wrap((BPlusIO)io, def.getSchema());
        };
    }

    /** */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static BPlusIO<IndexSearchRow> wrap(BPlusIO<IndexSearchRow> io, SortedIndexSchema schema) {
        assert io instanceof InlineIO;

        if (io instanceof BPlusInnerIO) {
            assert io instanceof AbstractInlineInnerIO
                || io instanceof InlineInnerIO;

            return new BPlusInnerIoDelegate((BPlusInnerIO<IndexSearchRow>)io, schema);
        }
        else {
            assert io instanceof AbstractInlineLeafIO
                || io instanceof InlineLeafIO;

            return new BPlusLeafIoDelegate((BPlusLeafIO<IndexSearchRow>)io, schema);
        }
    }

    /** */
    private static <T extends BPlusIO<IndexSearchRow> & InlineIO> IndexSearchRow lookupRow(
        SortedIndexSchema schema,
        long pageAddr,
        int idx,
        T io
    ) throws IgniteCheckedException {
        long link = io.getLink(pageAddr, idx);

        int off = io.offset(idx);

        Object[] keys = new Object[schema.getKeyDefinitions().length];

        int fieldOff = 0;

        for (int i = 0; i < schema.getKeyDefinitions().length; i++) {
            int type = schema.getKeyDefinitions()[i].getIdxType();

            InlineIndexKeyType keyType = InlineIndexKeyTypeRegistry.get(type);

            Object key = keyType.get(pageAddr, off + fieldOff, io.getInlineSize() - fieldOff);

            fieldOff += keyType.inlineSize(key);

            keys[i] = key;
        }

        return new IndexRowImpl(schema, new CacheDataRowAdapter(link), keys);
    }

    /** */
    private static class BPlusInnerIoDelegate<IO extends BPlusInnerIO<IndexSearchRow> & InlineIO>
        extends BPlusInnerIO<IndexSearchRow> implements InlineIO {
        /** */
        private final IO io;

        /** */
        private final SortedIndexSchema schema;

        /** */
        public BPlusInnerIoDelegate(IO io, SortedIndexSchema schema) {
            super(io.getType(), io.getVersion(), io.canGetRow(), io.getItemSize());
            this.io = io;
            this.schema = schema;
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, IndexSearchRow row) throws IgniteCheckedException {
            io.storeByOffset(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<IndexSearchRow> srcIo, long srcPageAddr, int srcIdx)
            throws IgniteCheckedException
        {
            io.store(dstPageAddr, dstIdx, srcIo, srcPageAddr, srcIdx);
        }

        /** {@inheritDoc} */
        @Override public IndexSearchRow getLookupRow(BPlusTree<IndexSearchRow, ?> tree, long pageAddr, int idx) throws IgniteCheckedException {
            return lookupRow(schema, pageAddr, idx, this);
        }

        /** {@inheritDoc} */
        @Override public long getLink(long pageAddr, int idx) {
            return io.getLink(pageAddr, idx);
        }

        /** {@inheritDoc} */
        @Override public int getInlineSize() {
            return io.getInlineSize();
        }
    }

    /** */
    private static class BPlusLeafIoDelegate<IO extends BPlusLeafIO<IndexSearchRow> & InlineIO>
        extends BPlusLeafIO<IndexSearchRow> implements InlineIO {
        /** */
        private final IO io;

        /** */
        private final SortedIndexSchema schema;

        /** */
        public BPlusLeafIoDelegate(IO io, SortedIndexSchema schema) {
            super(io.getType(), io.getVersion(), io.getItemSize());
            this.io = io;
            this.schema = schema;
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, IndexSearchRow row) throws IgniteCheckedException {
            io.storeByOffset(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<IndexSearchRow> srcIo, long srcPageAddr, int srcIdx)
            throws IgniteCheckedException
        {
            io.store(dstPageAddr, dstIdx, srcIo, srcPageAddr, srcIdx);
        }

        /** {@inheritDoc} */
        @Override public IndexSearchRow getLookupRow(BPlusTree<IndexSearchRow, ?> tree, long pageAddr, int idx) throws IgniteCheckedException {
            return lookupRow(schema, pageAddr, idx, this);
        }

        /** {@inheritDoc} */
        @Override public long getLink(long pageAddr, int idx) {
            return io.getLink(pageAddr, idx);
        }

        /** {@inheritDoc} */
        @Override public int getInlineSize() {
            return io.getInlineSize();
        }
    }
}
