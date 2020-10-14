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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.cache.query.index.IndexDefinition;
import org.apache.ignite.cache.query.index.IndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;

/**
 * Factory to create {@link InlineIndex}.
 */
public class InlineIndexFactory implements IndexFactory {
    /** */
    private final IgniteLogger log;

    /** */
    public InlineIndexFactory(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public Index createIndex(IndexDefinition def) {
        SortedIndexDefinition sdef = (SortedIndexDefinition) def;

        InlineIndexTree[] trees = new InlineIndexTree[sdef.getSegments()];
        InlineRecommender recommender = new InlineRecommender(log, sdef);

        try {
            for (int i = 0; i < sdef.getSegments(); ++i) {
                // Required for persistence.
                IgniteCacheDatabaseSharedManager db = def.getContext().shared().database();
                db.checkpointReadLock();

                try {
                    RootPage page = getRootPage(def.getContext(), sdef.getTreeName(), i);
                    trees[i] = createIndexSegment(sdef, page, recommender);

                } finally {
                    db.checkpointReadUnlock();
                }

            }

            return new InlineIndexImpl(sdef, trees);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private InlineIndexTree createIndexSegment(SortedIndexDefinition def, RootPage rootPage,
        InlineRecommender recommender) throws Exception {

        CacheGroupContext gctx = def.getContext().group();

        return new InlineIndexTree(
            def,
            gctx,
            def.getTreeName(),
            def.getContext().offheap().reuseListForIndex(def.getTreeName()),
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
