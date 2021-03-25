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
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandlerFactory;
import org.apache.ignite.internal.cache.query.index.sorted.MetaPageInfo;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineRecommender;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineLeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineLeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
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
    private final InlineIndexRowHandlerFactory rowHndFactory;

    /** */
    public DefragIndexFactory(IgniteCacheOffheapManager offheap, PageMemory newCachePageMemory, InlineIndex oldIdx) {
        // Row handler factory that produces no-op handler.
        rowHndFactory = (def, settings) -> oldIdx.segment(0).rowHandler();

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
            def.treeName(),
            offheap,
            offheap.reuseListForIndex(def.treeName()),
            newCachePageMemory,
            // Use old row handler to have access to inline index key types.
            pageIoResolver(),
            rootPage.pageId().pageId(),
            rootPage.isAllocated(),
            oldIdx.inlineSize(),
            def.keyTypeSettings(),
            null,
            stats,
            rowHndFactory,
            null
        );

        final MetaPageInfo oldInfo = oldIdx.segment(segmentNum).metaInfo();

        tree.copyMetaInfo(oldInfo);

        tree.enableSequentialWriteMode();

        return tree;
    }

    /** {@inheritDoc} */
    @Override protected RootPage rootPage(GridCacheContext<?, ?> ctx, String treeName, int segment) throws Exception {
        return offheap.rootPageForIndex(ctx.cacheId(), treeName, segment);
    }

    /** */
    private PageIoResolver pageIoResolver() {
        return pageAddr -> {
            PageIO io = PageIoResolver.DEFAULT_PAGE_IO_RESOLVER.resolve(pageAddr);

            if (io instanceof BPlusMetaIO)
                return io;

            //noinspection unchecked,rawtypes,rawtypes
            return wrap((BPlusIO)io, rowHndFactory.create(null, null));
        };
    }

    /** */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static BPlusIO<IndexRow> wrap(BPlusIO<IndexRow> io, InlineIndexRowHandler rowHnd) {
        assert io instanceof InlineIO;

        if (io instanceof BPlusInnerIO) {
            assert io instanceof AbstractInlineInnerIO
                || io instanceof InlineInnerIO;

            return new BPlusInnerIoDelegate((BPlusInnerIO<IndexRow>)io, rowHnd);
        }
        else {
            assert io instanceof AbstractInlineLeafIO
                || io instanceof InlineLeafIO;

            return new BPlusLeafIoDelegate((BPlusLeafIO<IndexRow>)io, rowHnd);
        }
    }

    /** */
    private static <T extends BPlusIO<IndexRow> & InlineIO> IndexRow lookupRow(
        InlineIndexRowHandler rowHnd,
        long pageAddr,
        int idx,
        T io
    ) {
        long link = io.link(pageAddr, idx);

        int off = io.offset(idx);

        IndexKey[] keys = new IndexKey[rowHnd.indexKeyDefinitions().size()];

        int fieldOff = 0;

        for (int i = 0; i < rowHnd.inlineIndexKeyTypes().size(); i++) {
            InlineIndexKeyType keyType = rowHnd.inlineIndexKeyTypes().get(i);

            IndexKey key = keyType.get(pageAddr, off + fieldOff, io.inlineSize() - fieldOff);

            fieldOff += keyType.inlineSize(key);

            keys[i] = key;
        }

        return new IndexRowImpl(rowHnd, new CacheDataRowAdapter(link), keys);
    }

    /** */
    private static class BPlusInnerIoDelegate<IO extends BPlusInnerIO<IndexRow> & InlineIO>
        extends BPlusInnerIO<IndexRow> implements InlineIO {
        /** */
        private final IO io;

        /** */
        private final InlineIndexRowHandler rowHnd;

        /** */
        private BPlusInnerIoDelegate(IO io, InlineIndexRowHandler rowHnd) {
            super(io.getType(), io.getVersion(), io.canGetRow(), io.getItemSize());
            this.io = io;
            this.rowHnd = rowHnd;
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, IndexRow row) throws IgniteCheckedException {
            io.storeByOffset(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<IndexRow> srcIo, long srcPageAddr, int srcIdx)
            throws IgniteCheckedException
        {
            io.store(dstPageAddr, dstIdx, srcIo, srcPageAddr, srcIdx);
        }

        /** {@inheritDoc} */
        @Override public IndexRow getLookupRow(BPlusTree<IndexRow, ?> tree, long pageAddr, int idx) throws IgniteCheckedException {
            return lookupRow(rowHnd, pageAddr, idx, this);
        }

        /** {@inheritDoc} */
        @Override public long link(long pageAddr, int idx) {
            return io.link(pageAddr, idx);
        }

        /** {@inheritDoc} */
        @Override public int inlineSize() {
            return io.inlineSize();
        }
    }

    /** */
    private static class BPlusLeafIoDelegate<IO extends BPlusLeafIO<IndexRow> & InlineIO>
        extends BPlusLeafIO<IndexRow> implements InlineIO {
        /** */
        private final IO io;

        /** */
        private final InlineIndexRowHandler rowHnd;

        /** */
        private BPlusLeafIoDelegate(IO io, InlineIndexRowHandler rowHnd) {
            super(io.getType(), io.getVersion(), io.getItemSize());
            this.io = io;
            this.rowHnd = rowHnd;
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, IndexRow row) throws IgniteCheckedException {
            io.storeByOffset(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<IndexRow> srcIo, long srcPageAddr, int srcIdx)
            throws IgniteCheckedException
        {
            io.store(dstPageAddr, dstIdx, srcIo, srcPageAddr, srcIdx);
        }

        /** {@inheritDoc} */
        @Override public IndexRow getLookupRow(BPlusTree<IndexRow, ?> tree, long pageAddr, int idx) throws IgniteCheckedException {
            return lookupRow(rowHnd, pageAddr, idx, this);
        }

        /** {@inheritDoc} */
        @Override public long link(long pageAddr, int idx) {
            return io.link(pageAddr, idx);
        }

        /** {@inheritDoc} */
        @Override public int inlineSize() {
            return io.inlineSize();
        }
    }
}
