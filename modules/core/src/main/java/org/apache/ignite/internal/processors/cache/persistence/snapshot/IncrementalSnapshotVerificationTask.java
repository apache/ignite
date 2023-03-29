/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.discovery.ConsistentIdMapper.ALL_NODES;

/** */
@GridInternal
public class IncrementalSnapshotVerificationTask extends AbstractSnapshotVerificationTask {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public SnapshotPartitionsVerifyTaskResult reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<Object, Map<Object, TransactionsHashRecord>> nodeTxHashMap = new HashMap<>();

        List<List<TransactionsHashRecord>> txHashConflicts = new ArrayList<>();

        Map<ClusterNode, Exception> errors = new HashMap<>();

        for (ComputeJobResult nodeRes: results) {
            if (nodeRes.getException() != null) {
                errors.put(nodeRes.getNode(), nodeRes.getException());

                continue;
            }

            Map<Object, TransactionsHashRecord> txHashRes = nodeRes.getData();

            if (log.isInfoEnabled())
                log.info("Handle VerifyIncrementalSnapshotJob result [node=" + nodeRes.getNode() + ", txHashRes=" + txHashRes + ']');

            nodeTxHashMap.put(nodeRes.getNode().consistentId(), txHashRes);

            Iterator<Map.Entry<Object, TransactionsHashRecord>> resIt = txHashRes.entrySet().iterator();

            while (resIt.hasNext()) {
                Map.Entry<Object, TransactionsHashRecord> nodeTxHash = resIt.next();

                Map<Object, TransactionsHashRecord> prevNodeTxHash = nodeTxHashMap.get(nodeTxHash.getKey());

                if (prevNodeTxHash != null) {
                    TransactionsHashRecord hash = nodeTxHash.getValue();
                    TransactionsHashRecord prevHash = prevNodeTxHash.remove(hash.localConsistentId());

                    if (prevHash == null || prevHash.transactionHash() != hash.transactionHash())
                        txHashConflicts.add(F.asList(hash, prevHash));

                    resIt.remove();
                }
            }
        }

        // Add all missed pairs to conflicts.
        nodeTxHashMap.values().stream()
            .flatMap(e -> e.values().stream())
            .forEach(e -> txHashConflicts.add(F.asList(e, null)));

        return new SnapshotPartitionsVerifyTaskResult(
            metas,
            errors.isEmpty() ? new IdleVerifyResultV2(txHashConflicts) : new IdleVerifyResultV2(errors));
    }

    /** {@inheritDoc} */
    @Override protected ComputeJob createJob(String name, @Nullable String path, int incIdx, String constId, Collection<String> groups) {
        return new VerifyIncrementalSnapshotJob(name, path, incIdx, constId);
    }

    /** */
    private static class VerifyIncrementalSnapshotJob extends ComputeJobAdapter {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Snapshot name to validate. */
        private final String snpName;

        /** Snapshot directory path. */
        private final String snpPath;

        /** Incremental snapshot index. */
        private final int incIdx;

        /** Consistent ID. */
        private final String consId;

        /**
         * @param snpName Snapshot name.
         * @param snpPath Snapshot directory path.
         * @param incIdx Incremental snapshot index.
         * @param consId Consistent ID.
         */
        public VerifyIncrementalSnapshotJob(
            String snpName,
            @Nullable String snpPath,
            int incIdx,
            String consId
        ) {
            this.snpName = snpName;
            this.snpPath = snpPath;
            this.incIdx = incIdx;
            this.consId = consId;
        }

        /**
         * @return Map containing calculated transactions hash for every remote node in the cluster.
         */
        @Override public Map<Object, TransactionsHashRecord> execute() throws IgniteException {
            try {
                if (log.isInfoEnabled()) {
                    log.info("Verify incremental snapshot procedure has been initiated " +
                        "[snpName=" + snpName + ", incrementIndex=" + incIdx + ", consId=" + consId + ']');
                }

                if (incIdx <= 0)
                    return Collections.emptyMap();

                BaselineTopology blt = ignite.context().state().clusterState().baselineTopology();

                checkBaseline(blt);

                IncrementalSnapshotProcessor proc = new IncrementalSnapshotProcessor(
                    ignite.context().cache().context(), snpName, snpPath, incIdx, null
                ) {
                    @Override void totalWalSegments(int segCnt) {
                        // No-op.
                    }

                    @Override void processedWalSegments(int segCnt) {
                        // No-op.
                    }

                    @Override void initWalEntries(LongAdder entriesCnt) {
                        // No-op.
                    }
                };

                Object locConsistentId = ignite.context().config().getConsistentId();
                short locShortId = blt.consistentIdMapping().get(locConsistentId);

                Set<Short> allShortIds = blt.compactIdMapping().keySet();

                Map<GridCacheVersion, Set<Short>> txs = new HashMap<>();
                Map<Short, Integer> nodesTxHash = new HashMap<>();

                BiConsumer<GridCacheVersion, Set<Short>> calcTransactionHash = (xid, partNodes) -> {
                    for (short shortId: partNodes) {
                        if (shortId != locShortId)
                            nodesTxHash.compute(shortId, (id, hash) -> xid.hashCode() + (hash == null ? 0 : hash));
                    }
                };

                proc.process(null, txRec -> {
                    if (txRec.state() == TransactionState.PREPARED) {
                        Set<Short> partNodes = new HashSet<>();

                        for (Map.Entry<Short, Collection<Short>> partNode: txRec.participatingNodes().entrySet()) {
                            partNodes.add(partNode.getKey());
                            partNodes.addAll(partNode.getValue());
                        }

                        if (partNodes.contains(ALL_NODES))
                            partNodes = allShortIds;

                        if (partNodes.contains(locShortId))
                            txs.put(txRec.nearXidVersion(), partNodes);
                    }
                    else if (txRec.state() == TransactionState.COMMITTED) {
                        Set<Short> partNodes = txs.remove(txRec.nearXidVersion());

                        // Legal cases:
                        // 1. This node is a transaction near node, but not primary or backup node.
                        // 2. This node participated in the transaction multiple times (e.g., primary for one key and backup for other key).
                        // 3. A transaction is included into previous incremental snapshot.
                        if (partNodes == null)
                            return;

                        calcTransactionHash.accept(txRec.nearXidVersion(), partNodes);
                    }
                    else if (txRec.state() == TransactionState.ROLLED_BACK)
                        txs.remove(txRec.nearXidVersion());
                });

                // All active transactions that didn't log COMMITTED or ROLL_BACK records are considered committed.
                // It is possible as incremental snapshot started after transaction left IgniteTxManager#activeTransactions() collection,
                // but completed before the final TxRecord was written.
                for (Map.Entry<GridCacheVersion, Set<Short>> tx: txs.entrySet())
                    calcTransactionHash.accept(tx.getKey(), tx.getValue());

                return nodesTxHash.entrySet().stream()
                    .map(e -> new TransactionsHashRecord(locConsistentId, blt.compactIdMapping().get(e.getKey()), e.getValue()))
                    .collect(Collectors.toMap(TransactionsHashRecord::remoteConsistentId, Function.identity()));
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** Checks that current baseline topology matches baseline topology of the snapshot. */
        private void checkBaseline(BaselineTopology blt) throws IgniteCheckedException, IOException {
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            File snpDir = snpMgr.snapshotLocalDir(snpName, snpPath);
            SnapshotMetadata meta = snpMgr.readSnapshotMetadata(snpDir, ignite.localNode().consistentId().toString());

            String errMsg = "Topologies of snapshot and current cluster are different [snp=" +
                meta.baselineNodes() + ", current=" + blt.consistentIds() + ']';

            if (blt.size() != meta.baselineNodes().size())
                throw new IgniteCheckedException(errMsg);

            for (Object consistentId: blt.consistentIds()) {
                if (!meta.baselineNodes().contains(consistentId.toString()))
                    throw new IgniteCheckedException(errMsg);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            VerifyIncrementalSnapshotJob job = (VerifyIncrementalSnapshotJob)o;

            return snpName.equals(job.snpName) && Objects.equals(incIdx, job.incIdx) && Objects.equals(snpPath, job.snpPath)
                && consId.equals(job.consId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(snpName, incIdx, snpPath, consId);
        }
    }
}
