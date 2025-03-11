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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    public CompletableFuture<IncrementalSnapshotCheckResult> checkIncrementalSnapshot(
        SnapshotFileTree sft,
        int incIdx
    ) {
        assert incIdx > 0;

        return CompletableFuture.supplyAsync(
            () -> new IncrementalSnapshotVerificationTask(kctx.grid(), log, sft, incIdx).execute(),
            executor);
    }

    /** */
    public IdleVerifyResult reduceIncrementalResults(
        SnapshotFileTree sft,
        int incIdx,
        Map<ClusterNode, IncrementalSnapshotCheckResult> results,
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
        @Nullable Collection<String> groups,
        boolean check
    ) {
        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        // The handlers use or may use the same snapshot pool. If it configured with 1 thread, launching waiting task in
        // the same pool might block it.
        return CompletableFuture.supplyAsync(() -> {
            try {
                return snpMgr.handlers().invokeAll(SnapshotHandlerType.RESTORE,
                    new SnapshotHandlerContext(meta, groups, kctx.cluster().get().localNode(), sft, false, check));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to call custom snapshot validation handlers.", e);
            }
        });
    }

    /** Launches local partitions checking. */
    public CompletableFuture<Map<PartitionKey, PartitionHashRecord>> checkPartitions(
        SnapshotMetadata meta,
        SnapshotFileTree sft,
        @Nullable Collection<String> groups,
        boolean forCreation,
        boolean checkParts,
        boolean skipPartsHashes
    ) {
        // Await in the default executor to avoid blocking the snapshot executor if it has just one thread.
        return CompletableFuture.supplyAsync(() -> {
                SnapshotHandlerContext hctx = new SnapshotHandlerContext(meta, groups, kctx.cluster().get().localNode(), sft, false, checkParts);
                try {
                    return new SnapshotPartitionsVerifyHandler(kctx.cache().context()).invoke(hctx);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            },
            executor);
    }

    /**
     * Checks results of all the snapshot validation handlres.
     * @param snpName Snapshot name
     * @param results Results: checking node -> snapshot part's consistend id -> custom handler name -> handler result.
     * @see #invokeCustomHandlers(SnapshotMetadata, SnapshotFileTree, Collection, boolean)
     */
    public void checkCustomHandlersResults(
        String snpName,
        Map<ClusterNode, Map<Object, Map<String, SnapshotHandlerResult<?>>>> results
    ) throws Exception {
        Map<String, List<SnapshotHandlerResult<?>>> clusterResults = new HashMap<>();
        Collection<UUID> execNodes = new ArrayList<>(results.size());

        // Checking node -> Map by snapshot part's consistend id.
        for (Map.Entry<ClusterNode, Map<Object, Map<String, SnapshotHandlerResult<?>>>> nodeRes : results.entrySet()) {
            // Consistent id -> Map by handler name.
            for (Map.Entry<Object, Map<String, SnapshotHandlerResult<?>>> nodeConsIdRes : nodeRes.getValue().entrySet()) {
                ClusterNode node = nodeRes.getKey();

                // We can get several different results from one node.
                execNodes.add(node.id());

                assert nodeRes.getValue() != null : "At least the default snapshot restore handler should have been executed ";

                // Handler name -> handler result.
                for (Map.Entry<String, SnapshotHandlerResult<?>> nodeHndRes : nodeConsIdRes.getValue().entrySet()) {
                    String hndName = nodeHndRes.getKey();
                    SnapshotHandlerResult<?> hndRes = nodeHndRes.getValue();

                    if (hndRes.error() != null)
                        throw hndRes.error();

                    clusterResults.computeIfAbsent(hndName, v -> new ArrayList<>()).add(hndRes);
                }
            }
        }

        kctx.cache().context().snapshotMgr().handlers().completeAll(SnapshotHandlerType.RESTORE, snpName, clusterResults,
            execNodes, wrns -> {});
    }
}
