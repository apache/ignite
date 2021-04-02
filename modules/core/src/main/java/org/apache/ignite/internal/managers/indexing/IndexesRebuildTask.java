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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheFuture;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexOperationCancellationException;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexOperationCancellationToken;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * Task that rebuilds indexes.
 */
public class IndexesRebuildTask {
    /** Index rebuilding futures for caches. Mapping: cacheId -> rebuild indexes future. */
    private final Map<Integer, SchemaIndexCacheFuture> idxRebuildFuts = new ConcurrentHashMap<>();

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
            Collection<InlineIndex> toRebuild = cctx.kernalContext().indexProcessor().treeIndexes(cctx, true);

            if (F.isEmpty(toRebuild))
                return null;

            clo = row -> cctx.kernalContext().indexProcessor().store(toRebuild, row, null, false);
        }

        // Closure prepared, do rebuild.
        cctx.kernalContext().query().markAsRebuildNeeded(cctx, true);

        GridFutureAdapter<Void> rebuildCacheIdxFut = new GridFutureAdapter<>();

        // To avoid possible data race.
        GridFutureAdapter<Void> outRebuildCacheIdxFut = new GridFutureAdapter<>();

        // An internal future for the ability to cancel index rebuilding.
        // This behavior should be discussed in IGNITE-14321.
        IgniteLogger log = cctx.kernalContext().grid().log();

        SchemaIndexCacheFuture intRebFut = new SchemaIndexCacheFuture(new SchemaIndexOperationCancellationToken());
        cancelIndexRebuildFuture(idxRebuildFuts.put(cctx.cacheId(), intRebFut), log);

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

            if (nonNull(err))
                U.error(log, "Failed to rebuild indexes for cache: " + cacheName, err);

            outRebuildCacheIdxFut.onDone(err);

            idxRebuildFuts.remove(cctx.cacheId(), intRebFut);
            intRebFut.onDone(err);
        });

        startRebuild(cctx, rebuildCacheIdxFut, clo, intRebFut.cancelToken());

        return outRebuildCacheIdxFut;
    }

    /** Actual start rebuilding. Use this method for test purposes only. */
    protected void startRebuild(GridCacheContext cctx, GridFutureAdapter<Void> fut,
        SchemaIndexCacheVisitorClosure clo, SchemaIndexOperationCancellationToken cancel) {
        new SchemaIndexCacheVisitorImpl(cctx, cancel, fut).visit(clo);
    }

    /**
     * Stop rebuilding indexes.
     *
     * @param cacheInfo Cache context info.
     */
    public void stopRebuild(GridCacheContextInfo cacheInfo, IgniteLogger log) {
        cancelIndexRebuildFuture(idxRebuildFuts.remove(cacheInfo.cacheId()), log);
    }

    /**
     * Cancel rebuilding indexes for the cache through a future.
     *
     * @param rebFut Index rebuilding future.
     */
    private void cancelIndexRebuildFuture(@Nullable SchemaIndexCacheFuture rebFut, IgniteLogger log) {
        if (rebFut != null && !rebFut.isDone() && rebFut.cancelToken().cancel()) {
            try {
                rebFut.get();
            }
            catch (IgniteCheckedException e) {
                if (!(e instanceof SchemaIndexOperationCancellationException))
                    log.warning("Error after canceling index rebuild.", e);
            }
        }
    }
}
