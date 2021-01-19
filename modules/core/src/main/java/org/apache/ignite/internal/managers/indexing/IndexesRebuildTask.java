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

package org.apache.ignite.internal.managers.indexing;

import java.util.Collection;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * Task that rebuilds indexes.
 */
public class IndexesRebuildTask {
    /** Start to rebuild. */
    public IgniteInternalFuture<?> rebuild(GridCacheContext cctx) {
        assert nonNull(cctx);

        if (!CU.affinityNode(cctx.localNode(), cctx.config().getNodeFilter()))
            return null;

        IgnitePageStoreManager pageStore = cctx.shared().pageStore();

        SchemaIndexCacheVisitorClosure clo;

        String cacheName = cctx.name();

        if (pageStore == null || !pageStore.hasIndexStore(cctx.groupId())) {
            boolean mvccEnabled = cctx.mvccEnabled();

            // If there are no index store, rebuild all indexes.
            clo = row -> cctx.queries().store(row, null, mvccEnabled);
        }
        else {
            Collection<InlineIndex> toRebuild = cctx.kernalContext().indexing().getTreeIndexes(cctx, true);

            if (F.isEmpty(toRebuild))
                return null;

            clo = row -> cctx.kernalContext().indexing().store(toRebuild, row, null, false);
        }

        // Closure prepared, do rebuild.
        cctx.kernalContext().query().markAsRebuildNeeded(cctx, true);

        GridFutureAdapter<Void> rebuildCacheIdxFut = new GridFutureAdapter<>();

        // To avoid possible data race.
        GridFutureAdapter<Void> outRebuildCacheIdxFut = new GridFutureAdapter<>();

        rebuildCacheIdxFut.listen(fut -> {
            Throwable err = fut.error();

            if (isNull(err)) {
                try {
                    cctx.kernalContext().query().markAsRebuildNeeded(cctx, false);
                }
                catch (Throwable t) {
                    err = t;
                }
            }

            if (nonNull(err)) {
                IgniteLogger log = cctx.kernalContext().grid().log();
                U.error(log, "Failed to rebuild indexes for cache: " + cacheName, err);
            }

            outRebuildCacheIdxFut.onDone(err);
        });

        startRebuild(cctx, rebuildCacheIdxFut, clo);

        return outRebuildCacheIdxFut;
    }

    /** Actual start rebuilding. Use this method for test purposes only. */
    protected void startRebuild(GridCacheContext cctx, GridFutureAdapter<Void> fut, SchemaIndexCacheVisitorClosure clo) {
        new SchemaIndexCacheVisitorImpl(cctx, null, fut).visit(clo);
    }
}
