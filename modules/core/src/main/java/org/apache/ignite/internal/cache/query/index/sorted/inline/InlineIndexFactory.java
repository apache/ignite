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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.IndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderIndex;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;

import static org.apache.ignite.internal.metric.IoStatisticsType.SORTED_INDEX;

/**
 * Factory to create {@link InlineIndex}.
 */
public class InlineIndexFactory implements IndexFactory {
    /** */
    public static final InlineIndexFactory INSTANCE = new InlineIndexFactory();

    /** {@inheritDoc} */
    @Override public Index createIndex(GridCacheContext<?, ?> cctx, IndexDefinition def) {
        SortedIndexDefinition sdef = (SortedIndexDefinition)def;

        InlineIndexTree[] trees = new InlineIndexTree[sdef.segments()];
        InlineRecommender recommender = new InlineRecommender(cctx, sdef);

        IoStatisticsHolderIndex stats = new IoStatisticsHolderIndex(
            SORTED_INDEX,
            cctx.name(),
            sdef.idxName().idxName(),
            cctx.kernalContext().metric()
        );

        try {
            for (int i = 0; i < sdef.segments(); ++i) {
                // Required for persistence.
                IgniteCacheDatabaseSharedManager db = cctx.shared().database();
                db.checkpointReadLock();

                try {
                    RootPage page = rootPage(cctx, sdef.treeName(), i);

                    trees[i] = createIndexSegment(cctx, sdef, page, stats, recommender, i);
                }
                finally {
                    db.checkpointReadUnlock();
                }
            }

            return new InlineIndexImpl(cctx, sdef, trees, stats);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    protected InlineIndexTree createIndexSegment(GridCacheContext<?, ?> cctx, SortedIndexDefinition def,
        RootPage rootPage, IoStatisticsHolder stats, InlineRecommender recommender, int segmentNum) throws Exception {
        return new InlineIndexTree(
            def,
            cctx.group(),
            def.treeName(),
            cctx.offheap(),
            cctx.offheap().reuseListForIndex(def.treeName()),
            cctx.dataRegion().pageMemory(),
            PageIoResolver.DEFAULT_PAGE_IO_RESOLVER,
            rootPage.pageId().pageId(),
            rootPage.isAllocated(),
            def.inlineSize(),
            cctx.config().getSqlIndexMaxInlineSize(),
            def.keyTypeSettings(),
            def.idxRowCache(),
            stats,
            def.rowHandlerFactory(),
            recommender);
    }

    /** */
    protected RootPage rootPage(GridCacheContext<?, ?> ctx, String treeName, int segment) throws Exception {
        return ctx.offheap().rootPageForIndex(ctx.cacheId(), treeName, segment);
    }
}
