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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexSchema;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineLeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineLeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.ThreadLocalSchemaHolder;
import org.apache.ignite.internal.managers.indexing.GridIndexingManager;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.LinkMap;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.TreeIterator;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.util.collection.IntMap;

/**
 *
 */
public class IndexingDefragmentation {
    /** Indexing. */
    private final GridIndexingManager indexing;

    /** Constructor. */
    public IndexingDefragmentation(GridIndexingManager indexing) {
        this.indexing = indexing;
    }

    /**
     * Defragment index partition.
     *
     * @param grpCtx Old group context.
     * @param newCtx New group context.
     * @param partPageMem Partition page memory.
     * @param mappingByPartition Mapping page memory.
     * @param cpLock Defragmentation checkpoint read lock.
     * @param cancellationChecker Cancellation checker.
     * @param log Log.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void defragment(
        CacheGroupContext grpCtx,
        CacheGroupContext newCtx,
        PageMemoryEx partPageMem,
        IntMap<LinkMap> mappingByPartition,
        CheckpointTimeoutLock cpLock,
        Runnable cancellationChecker,
        IgniteLogger log
    ) throws IgniteCheckedException {
        int pageSize = grpCtx.cacheObjectContext().kernalContext().grid().configuration().getDataStorageConfiguration().getPageSize();

        TreeIterator treeIterator = new TreeIterator(pageSize);

        PageMemoryEx oldCachePageMem = (PageMemoryEx)grpCtx.dataRegion().pageMemory();

        PageMemory newCachePageMemory = partPageMem;

        long cpLockThreshold = 150L;

        cpLock.checkpointReadLock();

        try {
            AtomicLong lastCpLockTs = new AtomicLong(System.currentTimeMillis());

            for (GridCacheContext cctx: grpCtx.caches()) {
                cancellationChecker.run();

                List<InlineIndex> indexes = indexing.getTreeIndexes(cctx, false);

                for (InlineIndex oldIdx: indexes) {
                    SortedIndexDefinition idxDef = (SortedIndexDefinition) indexing.getIndexDefition(oldIdx.id());

                    int segments = oldIdx.segmentsCount();

                    PageIoResolver pageIoRslvr = pageAddr -> {
                        PageIO io = PageIoResolver.DEFAULT_PAGE_IO_RESOLVER.resolve(pageAddr);

                        if (io instanceof BPlusMetaIO)
                            return io;

                        //noinspection unchecked,rawtypes,rawtypes
                        return wrap((BPlusIO)io, idxDef.getSchema());
                    };

                    for (int i = 0; i < segments; ++i) {
                        RootPage rootPage = newCtx.offheap().rootPageForIndex(cctx.cacheId(), idxDef.getTreeName(), i);

                        InlineIndexTree newTree = new InlineIndexTree(
                            idxDef,
                            cctx,
                            idxDef.getTreeName(),
                            newCtx.offheap(),
                            newCtx.offheap().reuseListForIndex(idxDef.getTreeName()),
                            newCachePageMemory,
                            pageIoRslvr,
                            rootPage.pageId().pageId(),
                            rootPage.isAllocated(),
                            oldIdx.inlineSize(),
                            null
                        );

                        newTree.enableSequentialWriteMode();

                        treeIterator.iterate(oldIdx.getSegment(i), oldCachePageMem, (theTree, io, pageAddr, idx) -> {
                            cancellationChecker.run();

                            if (System.currentTimeMillis() - lastCpLockTs.get() >= cpLockThreshold) {
                                cpLock.checkpointReadUnlock();

                                cpLock.checkpointReadLock();

                                lastCpLockTs.set(System.currentTimeMillis());
                            }

                            assert 1 == io.getVersion()
                                : "IO version " + io.getVersion() + " is not supported by current defragmentation algorithm." +
                                " Please implement copying of tree in a new format.";

                            BPlusIO<IndexSearchRow> h2IO = wrap(io, idxDef.getSchema());

                            IndexSearchRow row = theTree.getRow(h2IO, pageAddr, idx);

                            if (row instanceof IndexRowImpl) {
                                IndexRowImpl r = (IndexRowImpl) row;

                                CacheDataRow cacheDataRow = r.getCacheDataRow();

                                int partition = cacheDataRow.partition();

                                long link = r.getLink();

                                LinkMap map = mappingByPartition.get(partition);

                                long newLink = map.get(link);

                                IndexRowImpl newRow = new IndexRowImpl(
                                    idxDef.getSchema(), new CacheDataRowAdapter(newLink), r.keys());

                                newTree.putx(newRow);
                            }

                            return true;
                        });
                    }
                }
            }
        }
        finally {
            cpLock.checkpointReadUnlock();
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
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static BPlusIO<IndexSearchRow> wrap(BPlusIO<IndexSearchRow> io, SortedIndexSchema schema) {
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
            ThreadLocalSchemaHolder.setSchema(schema);

            try {
                io.storeByOffset(pageAddr, off, row);

            } finally {
                ThreadLocalSchemaHolder.cleanSchema();
            }        }

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
            ThreadLocalSchemaHolder.setSchema(schema);

            try {
                io.storeByOffset(pageAddr, off, row);

            } finally {
                ThreadLocalSchemaHolder.cleanSchema();
            }
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
