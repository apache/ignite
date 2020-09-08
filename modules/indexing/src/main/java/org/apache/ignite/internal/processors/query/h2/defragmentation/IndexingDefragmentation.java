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

package org.apache.ignite.internal.processors.query.h2.defragmentation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CacheDefragmentationContext;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.GridQueryIndexingDefragmentation;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.LinkMap;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.TreeIterator;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.h2.index.Index;

/**
 *
 */
public class IndexingDefragmentation implements GridQueryIndexingDefragmentation {
    /** Indexing. */
    private final IgniteH2Indexing indexing;

    /** Constructor. */
    public IndexingDefragmentation(IgniteH2Indexing indexing) {
        this.indexing = indexing;
    }

    /** {@inheritDoc} */
    @Override public void defragmentate(
        CacheGroupContext grpCtx,
        CacheGroupContext newCtx,
        CacheDefragmentationContext defrgCtx,
        Map<Integer, LinkMap> mappingByPartition,
        IgniteLogger log) throws IgniteCheckedException {
        int pageSize = grpCtx.cacheObjectContext().kernalContext().grid().configuration().getDataStorageConfiguration().getPageSize();

        TreeIterator treeIterator = new TreeIterator(pageSize);

        PageMemoryEx oldCachePageMem = (PageMemoryEx)grpCtx.dataRegion().pageMemory();

        PageMemory newCachePageMemory = defrgCtx.partitionsDataRegion().pageMemory();

        Collection<GridH2Table> tables = indexing.schemaManager().dataTables();

        for (GridH2Table table : tables) {
            GridH2RowDescriptor rowDescriptor = table.rowDescriptor();

            ArrayList<Index> indexes = table.getIndexes();
            H2TreeIndex index = (H2TreeIndex) indexes.get(2);

            GridCacheContext cctx = table.cacheContext();

            int segments = index.segmentsCount();

            H2Tree firstTree = index.treeForRead(0);

            H2TreeIndex newIndex = H2TreeIndex.createIndex(
                cctx,
                null,
                table,
                index.getName(),
                firstTree.getPk(),
                firstTree.getAffinityKey(),
                Arrays.asList(firstTree.cols()),
                Arrays.asList(firstTree.cols()),
                index.inlineSize(),
                segments,
                newCachePageMemory,
                (segIdx, treeName) -> newCtx.offheap().rootPageForIndex(cctx.cacheId(), treeName, segIdx),
                log
            );

            for (int i = 0; i < segments; i++) {
                H2Tree tree = index.treeForRead(i);

                treeIterator.iterate(tree, oldCachePageMem, (theTree, io, pageAddr, idx) -> {
                    H2Row row = theTree.getRow(io, pageAddr, idx);

                    if (row instanceof H2CacheRow) {
                        H2CacheRow h2CacheRow = (H2CacheRow) row;

                        CacheDataRow cacheDataRow = h2CacheRow.getRow();

                        int partition = cacheDataRow.partition();
                        long link = h2CacheRow.link();
                        LinkMap map = mappingByPartition.get(partition);
                        long newLink = map.get(link);

                        CacheDataRowAdapter newDataRow = new CacheDataRowAdapter(cacheDataRow.key(), cacheDataRow.value(), cacheDataRow.version(), cacheDataRow.expireTime()) {
                            @Override
                            public void link(long link) {
                                this.link = link;
                            }
                        };

                        H2CacheRow newRow = new H2CacheRow(rowDescriptor, newDataRow);

                        newRow.link(newLink);

                        newIndex.putx(newRow);
                    }

                    return true;
                });
            }
        }
    }

}
