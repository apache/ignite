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
package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.management.cache.PartitionKey;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.jetbrains.annotations.Nullable;

/** */
public class SnapshotChecker {
    /** */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext kctx;

    /** */
    private final ExecutorService executor;

    /** */
    public SnapshotChecker(GridKernalContext kctx) {
        this.kctx = kctx;

        executor = kctx.pools().getSnapshotExecutorService();

        log = kctx.log(getClass());
    }

    /** Launches local metas checking. */
    public CompletableFuture<List<SnapshotMetadata>> checkLocalMetas(
        SnapshotFileTree sft,
        int incIdx,
        @Nullable Collection<Integer> grpIds
    ) {
        return CompletableFuture.supplyAsync(() ->
            new SnapshotMetadataVerificationTask(kctx.grid(), log, sft, incIdx, grpIds).execute(), executor);
    }

    /** */
    public CompletableFuture<IncrementalSnapshotVerificationTaskResult> checkIncrementalSnapshot(
        SnapshotFileTree sft,
        int incIdx
    ) {
        assert incIdx > 0;

        return CompletableFuture.supplyAsync(
            () -> new IncrementalSnapshotVerificationTask(kctx.grid(), log, sft, incIdx).execute(),
            executor
        );
    }

    /** */
    public IdleVerifyResult reduceIncrementalResults(
        SnapshotFileTree sft,
        int incIdx,
        Map<ClusterNode, IncrementalSnapshotVerificationTaskResult> results,
        Map<ClusterNode, Exception> operationErrors
    ) {
        if (!operationErrors.isEmpty())
            return IdleVerifyResult.builder().exceptions(operationErrors).build();

        return new IncrementalSnapshotVerificationTask(kctx.grid(), log, sft, incIdx).reduce(results);
    }

    /** */
    public Map<ClusterNode, Exception> reduceMetasResults(SnapshotFileTree sft, Map<ClusterNode, List<SnapshotMetadata>> metas) {
        return new SnapshotMetadataVerificationTask(kctx.grid(), log, sft, 0, null).reduce(metas);
    }

    /**
     * Calls all the registered custom validaton handlers.
     *
     * @see IgniteSnapshotManager#handlers()
     */
    public CompletableFuture<Map<String, SnapshotHandlerResult<Object>>> invokeCustomHandlers(
        SnapshotMetadata meta,
        SnapshotFileTree sft,
        @Nullable Collection<String> grps,
        boolean check
    ) {
        // The handlers use or may use the same snapshot pool. If it is configured with 1 thread, launching waiting task in
        // the same pool might block it.
        return CompletableFuture.supplyAsync(() ->
            new SnapshotHandlerRestoreTask(kctx.grid(), log, sft, grps, check).execute()
        );
    }

    /** Launches local partitions checking. */
    public CompletableFuture<Map<PartitionKey, PartitionHashRecord>> checkPartitions(
        SnapshotMetadata meta,
        SnapshotFileTree sft,
        @Nullable Collection<String> grps,
        boolean forCreation,
        boolean checkParts
    ) {
        // Await in the default executor to avoid blocking the snapshot executor if it has just one thread.
        return CompletableFuture.supplyAsync(() -> {
            SnapshotHandlerContext hctx = new SnapshotHandlerContext(
                meta, grps, kctx.cluster().get().localNode(), sft, false, checkParts);

            try {
                return new SnapshotPartitionsVerifyHandler(kctx.cache().context()).invoke(hctx);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }, executor);
    }

    /**
     * Checks results of all the snapshot validation handlres.
     * @param snpName Snapshot name.
     * @param results Results: checking node -> snapshot part's consistend id -> custom handler name -> handler result.
     * @see #invokeCustomHandlers(SnapshotMetadata, SnapshotFileTree, Collection, boolean)
     */
    public void checkCustomHandlersResults(
        String snpName,
        Map<ClusterNode, Map<Object, Map<String, SnapshotHandlerResult<?>>>> results
    ) {
        new SnapshotHandlerRestoreTask(kctx.grid(), log, null, null, true).reduce(snpName, results);
    }
}
