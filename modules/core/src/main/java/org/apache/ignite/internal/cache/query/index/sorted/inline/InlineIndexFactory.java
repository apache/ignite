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

import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.cache.query.index.IndexDefinition;
import org.apache.ignite.cache.query.index.IndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;

/**
 * Factory to create {@link InlineIndex}.
 */
public class InlineIndexFactory implements IndexFactory {
    /** Instance of factory. */
    public static final InlineIndexFactory INSTANCE = new InlineIndexFactory();

    /** No-op constructor. */
    private InlineIndexFactory() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Index createIndex(GridCacheContext cctx, IndexDefinition def) {
        SortedIndexDefinition sdef = (SortedIndexDefinition) def;

        InlineIndexTree[] trees = new InlineIndexTree[sdef.getSegments()];
        InlineRecommender recommender = new InlineRecommender(cctx, sdef);

        try {
            for (int i = 0; i < sdef.getSegments(); ++i) {
                // Required for persistence.
                IgniteCacheDatabaseSharedManager db = cctx.shared().database();
                db.checkpointReadLock();

                try {
                    RootPage page = getRootPage(cctx, sdef.getTreeName(), i);

                    trees[i] = createIndexSegment(cctx, sdef, page, recommender);

                } finally {
                    db.checkpointReadUnlock();
                }
            }

            return new InlineIndexImpl(cctx, sdef, trees);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private InlineIndexTree createIndexSegment(GridCacheContext cctx, SortedIndexDefinition def, RootPage rootPage,
        InlineRecommender recommender) throws Exception {

        return new InlineIndexTree(
            def,
            cctx,
            def.getTreeName(),
            cctx.offheap(),
            cctx.offheap().reuseListForIndex(def.getTreeName()),
            cctx.dataRegion().pageMemory(),
            PageIoResolver.DEFAULT_PAGE_IO_RESOLVER,
            rootPage.pageId().pageId(),
            rootPage.isAllocated(),
            def.getInlineSize(),
            recommender);
    }

    /** */
    private RootPage getRootPage(GridCacheContext ctx, String treeName, int segment) throws Exception {
        return ctx.offheap().rootPageForIndex(ctx.cacheId(), treeName, segment);
    }
}
