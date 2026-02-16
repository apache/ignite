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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.management.cache.PartitionKey;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.util.typedef.F;
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
        return CompletableFuture.supplyAsync(
            new SnapshotMetadataVerificationTask(kctx.grid(), log, sft, incIdx, grpIds),
            executor
        );
    }

    /** */
    public CompletableFuture<IncrementalSnapshotVerifyResult> checkIncrementalSnapshot(
        SnapshotFileTree sft,
        int incIdx,
        @Nullable Consumer<Integer> totalCnsmr,
        @Nullable Consumer<Integer> checkedCnsmr
    ) {
        assert incIdx > 0;

        return CompletableFuture.supplyAsync(
            new IncrementalSnapshotVerify(kctx.grid(), log, sft, incIdx, totalCnsmr, checkedCnsmr),
            executor
        );
    }

    /** */
    public IdleVerifyResult reduceIncrementalResults(
        SnapshotFileTree sft,
        int incIdx,
        Map<ClusterNode, IncrementalSnapshotVerifyResult> results,
        Map<ClusterNode, Exception> operationErrors
    ) {
        if (!operationErrors.isEmpty())
            return IdleVerifyResult.builder().exceptions(operationErrors).build();

        IdleVerifyResult.Builder bldr = IdleVerifyResult.builder();

        for (Map.Entry<ClusterNode, IncrementalSnapshotVerifyResult> nodeRes: results.entrySet()) {
            IncrementalSnapshotVerifyResult res = nodeRes.getValue();

            if (!F.isEmpty(res.partiallyCommittedTxs()))
                bldr.addPartiallyCommited(nodeRes.getKey(), res.partiallyCommittedTxs());

            bldr.addPartitionHashes(res.partHashRes());

            if (log.isDebugEnabled())
                log.debug("Handle VerifyIncrementalSnapshotJob result [node=" + nodeRes.getKey() + ", taskRes=" + res + ']');

            bldr.addIncrementalHashRecords(nodeRes.getKey(), res.txHashRes());
        }

        return bldr.build();
    }

    /** */
    public Map<ClusterNode, Exception> reduceMetasResults(SnapshotFileTree sft, Map<ClusterNode, List<SnapshotMetadata>> results) {
        Map<ClusterNode, Exception> exs = new HashMap<>();

        SnapshotMetadata first = null;
        Set<String> baselineMetasLeft = Collections.emptySet();

        for (Map.Entry<ClusterNode, List<SnapshotMetadata>> res : results.entrySet()) {
            List<SnapshotMetadata> metas = res.getValue();

            for (SnapshotMetadata meta : metas) {
                if (first == null) {
                    first = meta;

                    baselineMetasLeft = new HashSet<>(meta.baselineNodes());
                }

                baselineMetasLeft.remove(meta.consistentId());

                if (!first.sameSnapshot(meta)) {
                    exs.put(res.getKey(),
                        new IgniteException("An error occurred during comparing snapshot metadata from cluster nodes " +
                            "[first=" + first + ", meta=" + meta + ", nodeId=" + res.getKey().id() + ']'));
                }
            }
        }

        if (first == null && exs.isEmpty()) {
            throw new IllegalArgumentException("Snapshot does not exists [snapshot=" + sft.name()
                + ", baseDir=" + sft.root() + ", consistentId=" + sft.consistentId() + ']');
        }

        if (!F.isEmpty(baselineMetasLeft) && F.isEmpty(exs)) {
            throw new IgniteException("No snapshot metadatas found for the baseline nodes " +
                "with consistent ids: " + String.join(", ", baselineMetasLeft));
        }

        return exs;
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
        boolean check,
        @Nullable Consumer<Integer> totalCnsmr,
        @Nullable Consumer<Integer> processedCnsmr
    ) {
        // The handlers use or may use the same snapshot pool. If it is configured with 1 thread, launching waiting task in
        // the same pool might block it.
        return CompletableFuture.supplyAsync(() -> {
                try {
                    SnapshotHandlerContext hndCnt = new SnapshotHandlerContext(
                        meta,
                        grps,
                        kctx.cluster().get().localNode(),
                        sft,
                        false,
                        check,
                        totalCnsmr == null ? null : (hndCls, totalCnt) -> totalCnsmr.accept(totalCnt),
                        processedCnsmr == null ? null : (hndCls, partId) -> processedCnsmr.accept(partId)
                    );

                    return kctx.cache().context().snapshotMgr().handlers().invokeAll(SnapshotHandlerType.RESTORE, hndCnt);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        );
    }

    /** Launches local partitions checking. */
    public CompletableFuture<Map<PartitionKey, PartitionHashRecord>> checkPartitions(
        SnapshotMetadata meta,
        SnapshotFileTree sft,
        @Nullable Collection<String> grps,
        boolean forCreation,
        boolean checkParts,
        @Nullable Consumer<Integer> totalCnsmr,
        @Nullable Consumer<Integer> checkedPartCnsmr
    ) {
        return CompletableFuture.supplyAsync(() -> {
            SnapshotHandlerContext hctx = new SnapshotHandlerContext(
                meta,
                grps,
                kctx.cluster().get().localNode(),
                sft,
                false,
                checkParts,
                totalCnsmr == null ? null : (hndCls, totalCnt) -> totalCnsmr.accept(totalCnt),
                checkedPartCnsmr == null ? null : (hndCls, partId) -> checkedPartCnsmr.accept(partId)
            );

            try {
                return new SnapshotPartitionsVerifyHandler(kctx.cache().context()).invoke(hctx);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }, executor);
    }
}
