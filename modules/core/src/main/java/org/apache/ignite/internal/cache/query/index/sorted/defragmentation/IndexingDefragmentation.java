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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.defragmentation.DefragIndexFactory.DefragIndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.MvccIO;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.LinkMap;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.TreeIterator;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;

/**
 *
 */
public class IndexingDefragmentation {
    /** Indexing. */
    private final IndexProcessor indexing;

    /** Constructor. */
    public IndexingDefragmentation(IndexProcessor indexing) {
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
     * @param defragmentationThreadPool Thread pool for defragmentation.
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
        IgniteThreadPoolExecutor defragmentationThreadPool,
        IgniteLogger log
    ) throws IgniteCheckedException {
        int pageSize = grpCtx.cacheObjectContext().kernalContext().grid().configuration().getDataStorageConfiguration().getPageSize();

        PageMemoryEx oldCachePageMem = (PageMemoryEx)grpCtx.dataRegion().pageMemory();

        PageMemory newCachePageMemory = partPageMem;

        Collection<TableIndexes> tables = tables(grpCtx);

        long cpLockThreshold = 150L;

        AtomicLong lastCpLockTs = new AtomicLong(System.currentTimeMillis());

        IgniteUtils.doInParallel(
            defragmentationThreadPool,
            tables,
            table -> defragmentTable(
                newCtx,
                mappingByPartition,
                cpLock,
                cancellationChecker,
                pageSize,
                oldCachePageMem,
                newCachePageMemory,
                cpLockThreshold,
                lastCpLockTs,
                table
            )
        );

        if (log.isInfoEnabled())
            log.info("Defragmentation indexes completed for group '" + grpCtx.groupId() + "'");
    }

    /**
     * Defragment one given table.
     */
    private boolean defragmentTable(
        CacheGroupContext newCtx,
        IntMap<LinkMap> mappingByPartition,
        CheckpointTimeoutLock cpLock,
        Runnable cancellationChecker,
        int pageSize,
        PageMemoryEx oldCachePageMem,
        PageMemory newCachePageMemory,
        long cpLockThreshold,
        AtomicLong lastCpLockTs,
        TableIndexes indexes
    ) throws IgniteCheckedException {
        cpLock.checkpointReadLock();

        try {
            TreeIterator treeIterator = new TreeIterator(pageSize);

            GridCacheContext<?, ?> cctx = indexes.cctx;

            cancellationChecker.run();

            for (InlineIndex oldIdx : indexes.idxs) {
                InlineIndexRowHandler oldRowHnd = oldIdx.segment(0).rowHandler();

                SortedIndexDefinition idxDef = (SortedIndexDefinition)indexing.indexDefinition(oldIdx.id());

                InlineIndexImpl newIdx = new DefragIndexFactory(newCtx.offheap(), newCachePageMemory, oldIdx)
                    .createIndex(cctx, idxDef)
                    .unwrap(InlineIndexImpl.class);

                int segments = oldIdx.segmentsCount();

                for (int i = 0; i < segments; ++i) {
                    treeIterator.iterate(oldIdx.segment(i), oldCachePageMem, (theTree, io, pageAddr, idx) -> {
                        cancellationChecker.run();

                        if (System.currentTimeMillis() - lastCpLockTs.get() >= cpLockThreshold) {
                            cpLock.checkpointReadUnlock();

                            cpLock.checkpointReadLock();

                            lastCpLockTs.set(System.currentTimeMillis());
                        }

                        assert 1 == io.getVersion()
                            : "IO version " + io.getVersion() + " is not supported by current defragmentation algorithm." +
                            " Please implement copying of tree in a new format.";

                        BPlusIO<IndexRow> h2IO = DefragIndexFactory.wrap(io, oldRowHnd);

                        IndexRow row = theTree.getRow(h2IO, pageAddr, idx);

                        if (row instanceof DefragIndexRowImpl) {
                            DefragIndexRowImpl r = (DefragIndexRowImpl)row;

                            CacheDataRow cacheDataRow = r.cacheDataRow();

                            int partition = cacheDataRow.partition();

                            long link = r.link();

                            LinkMap map = mappingByPartition.get(partition);

                            long newLink = map.get(link);

                            // Use old row handler, as MetaInfo is copied from old tree.
                            DefragIndexRowImpl newRow = DefragIndexRowImpl.create(
                                oldRowHnd,
                                newLink,
                                r,
                                ((MvccIO)io).storeMvccInfo()
                            );

                            newIdx.putIndexRow(newRow);
                        }

                        return true;
                    });
                }
            }

            return true;
        }
        catch (Throwable t) {
            newCtx.cacheObjectContext().kernalContext()
                .failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw t;
        }
        finally {
            cpLock.checkpointReadUnlock();
        }
    }

    /** Returns collection of table indexes. */
    private Collection<TableIndexes> tables(CacheGroupContext gctx) {
        Collection<TableIndexes> tables = new ArrayList<>();

        for (GridCacheContext<?, ?> cctx: gctx.caches()) {
            Map<String, TableIndexes> idxs = new HashMap<>();

            List<InlineIndex> indexes = indexing.treeIndexes(cctx.name(), false);

            for (InlineIndex idx: indexes) {
                String table = indexing.indexDefinition(idx.id()).idxName().tableName();

                idxs.putIfAbsent(table, new TableIndexes(cctx, table));

                idxs.get(table).addIndex(idx);
            }

            tables.addAll(idxs.values());
        }

        return tables;
    }

    /** Holder for indexes per cache table. */
    private static class TableIndexes {
        /** Table name. */
        final @Nullable String tableName;

        /** Cache context. */
        final GridCacheContext<?, ?> cctx;

        /** Indexes. */
        final List<InlineIndex> idxs = new ArrayList<>();

        /** */
        TableIndexes(GridCacheContext<?, ?> cctx, String tableName) {
            this.cctx = cctx;
            this.tableName = tableName;
        }

        /** */
        void addIndex(InlineIndex idx) {
            idxs.add(idx);
        }
    }
}
