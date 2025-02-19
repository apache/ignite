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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.management.cache.PartitionKey;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridLocalConfigManager;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.VerifyPartitionContext;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.discovery.ConsistentIdMapper.ALL_NODES;

/** */
@GridInternal
public class IncrementalSnapshotVerificationTask extends AbstractSnapshotVerificationTask {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public SnapshotPartitionsVerifyTaskResult reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<Object, Map<Object, TransactionsHashRecord>> nodeTxHashMap = new HashMap<>();

        List<List<TransactionsHashRecord>> txHashConflicts = new ArrayList<>();
        Map<PartitionKey, List<PartitionHashRecord>> partHashes = new HashMap<>();
        Map<ClusterNode, Collection<GridCacheVersion>> partiallyCommittedTxs = new HashMap<>();

        Map<ClusterNode, Exception> errors = new HashMap<>();

        for (ComputeJobResult nodeRes: results) {
            if (nodeRes.getException() != null) {
                errors.put(nodeRes.getNode(), nodeRes.getException());

                continue;
            }

            IncrementalSnapshotVerificationTaskResult res = nodeRes.getData();

            if (!F.isEmpty(res.exceptions())) {
                errors.put(nodeRes.getNode(), F.first(res.exceptions()));

                continue;
            }

            if (!F.isEmpty(res.partiallyCommittedTxs()))
                partiallyCommittedTxs.put(nodeRes.getNode(), res.partiallyCommittedTxs());

            for (Map.Entry<PartitionKey, PartitionHashRecord> entry: res.partHashRes().entrySet())
                partHashes.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(entry.getValue());

            if (log.isDebugEnabled())
                log.debug("Handle VerifyIncrementalSnapshotJob result [node=" + nodeRes.getNode() + ", taskRes=" + res + ']');

            nodeTxHashMap.put(nodeRes.getNode().consistentId(), res.txHashRes());

            Iterator<Map.Entry<Object, TransactionsHashRecord>> resIt = res.txHashRes().entrySet().iterator();

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
            errors.isEmpty() ?
                new IdleVerifyResult(partHashes, txHashConflicts, partiallyCommittedTxs)
                : new IdleVerifyResult(errors));
    }

    /** {@inheritDoc} */
    @Override protected VerifyIncrementalSnapshotJob createJob(
        String name,
        String folderName,
        String consId,
        SnapshotPartitionsVerifyTaskArg args
    ) {
        return new VerifyIncrementalSnapshotJob(name, args.snapshotPath(), args.incrementIndex(), folderName, consId);
    }

    /** */
    private static class VerifyIncrementalSnapshotJob extends AbstractSnapshotVerificationJob {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Incremental snapshot index. */
        private final int incIdx;

        /** */
        private LongAdder procEntriesCnt;

        /**
         * @param snpName Snapshot name.
         * @param snpPath Snapshot directory path.
         * @param incIdx Incremental snapshot index.
         * @param folderName Folder name for snapshot.
         * @param consId Consistent id of the related node.
         */
        public VerifyIncrementalSnapshotJob(
            String snpName,
            @Nullable String snpPath,
            int incIdx,
            String folderName,
            String consId
        ) {
            super(snpName, snpPath, folderName, consId, null, true);

            this.incIdx = incIdx;
        }

        /**
         * @return Map containing calculated transactions hash for every remote node in the cluster.
         */
        @Override public IncrementalSnapshotVerificationTaskResult execute0() throws IgniteException {
            try {
                if (log.isInfoEnabled()) {
                    log.info("Verify incremental snapshot procedure has been initiated " +
                        "[snpName=" + snpName + ", incrementIndex=" + incIdx + ", consId=" + consId + ']');
                }

                if (incIdx <= 0)
                    return new IncrementalSnapshotVerificationTaskResult();

                BaselineTopology blt = ignite.context().state().clusterState().baselineTopology();

                checkBaseline(blt);

                Map<Integer, StoredCacheData> txCaches = readTxCachesData();

                AtomicLong procSegCnt = new AtomicLong();

                IncrementalSnapshotProcessor proc = new IncrementalSnapshotProcessor(
                    ignite.context().cache().context(), sft, incIdx, txCaches.keySet()
                ) {
                    @Override void totalWalSegments(int segCnt) {
                        // No-op.
                    }

                    @Override void processedWalSegments(int segCnt) {
                        procSegCnt.set(segCnt);
                    }

                    @Override void initWalEntries(LongAdder entriesCnt) {
                        procEntriesCnt = entriesCnt;
                    }
                };

                short locNodeId = blt.consistentIdMapping().get(consId);

                Set<GridCacheVersion> activeDhtTxs = new HashSet<>();
                Map<GridCacheVersion, Set<Short>> txPrimParticipatingNodes = new HashMap<>();
                Map<Short, HashHolder> nodesTxHash = new HashMap<>();

                Set<GridCacheVersion> partiallyCommittedTxs = new HashSet<>();
                // Hashes in this map calculated based on WAL records only, not part-X.bin data.
                Map<PartitionKey, HashHolder> partMap = new HashMap<>();
                List<Exception> exceptions = new ArrayList<>();

                Function<Short, HashHolder> hashHolderBuilder = (k) -> new HashHolder();

                BiConsumer<GridCacheVersion, Set<Short>> calcTxHash = (xid, participatingNodes) -> {
                    for (short nodeId: participatingNodes) {
                        if (nodeId != locNodeId) {
                            HashHolder hash = nodesTxHash.computeIfAbsent(nodeId, hashHolderBuilder);

                            hash.increment(xid.hashCode(), 0);
                        }
                    }
                };

                // CacheId -> CacheGrpId.
                Map<Integer, Integer> cacheGrpId = txCaches.values().stream()
                    .collect(Collectors.toMap(
                        StoredCacheData::cacheId,
                        cacheData -> CU.cacheGroupId(cacheData.config().getName(), cacheData.config().getGroupName())
                    ));

                LongAdder procTxCnt = new LongAdder();

                proc.process(dataEntry -> {
                    if (dataEntry.op() == GridCacheOperation.READ || !exceptions.isEmpty())
                        return;

                    if (log.isTraceEnabled())
                        log.trace("Checking data entry [entry=" + dataEntry + ']');

                    if (!activeDhtTxs.contains(dataEntry.writeVersion()))
                        partiallyCommittedTxs.add(dataEntry.nearXidVersion());

                    StoredCacheData cacheData = txCaches.get(dataEntry.cacheId());

                    PartitionKey partKey = new PartitionKey(
                        cacheGrpId.get(dataEntry.cacheId()),
                        dataEntry.partitionId(),
                        CU.cacheOrGroupName(cacheData.config()));

                    HashHolder hash = partMap.computeIfAbsent(partKey, (k) -> new HashHolder());

                    try {
                        int valHash = dataEntry.key().hashCode();

                        if (dataEntry.value() != null)
                            valHash += Arrays.hashCode(dataEntry.value().valueBytes(null));

                        int verHash = dataEntry.writeVersion().hashCode();

                        hash.increment(valHash, verHash);
                    }
                    catch (IgniteCheckedException ex) {
                        exceptions.add(ex);
                    }
                }, txRec -> {
                    if (!exceptions.isEmpty())
                        return;

                    if (log.isDebugEnabled())
                        log.debug("Checking tx record [txRec=" + txRec + ']');

                    if (txRec.state() == TransactionState.PREPARED) {
                        // Collect only primary nodes. For some cases backup nodes is included into TxRecord#participationNodes()
                        // but actually doesn't even start transaction, for example, if the node participates only as a backup
                        // of reading only keys.
                        Set<Short> primParticipatingNodes = txRec.participatingNodes().keySet();

                        if (primParticipatingNodes.contains(locNodeId)) {
                            txPrimParticipatingNodes.put(txRec.nearXidVersion(), primParticipatingNodes);
                            activeDhtTxs.add(txRec.writeVersion());
                        }
                        else {
                            for (Collection<Short> backups: txRec.participatingNodes().values()) {
                                if (backups.contains(ALL_NODES) || backups.contains(locNodeId))
                                    activeDhtTxs.add(txRec.writeVersion());
                            }
                        }
                    }
                    else if (txRec.state() == TransactionState.COMMITTED) {
                        activeDhtTxs.remove(txRec.writeVersion());

                        Set<Short> participatingNodes = txPrimParticipatingNodes.remove(txRec.nearXidVersion());

                        // Legal cases:
                        // 1. This node is a transaction near node, but not primary or backup node.
                        // 2. This node participated in the transaction multiple times (e.g., primary for one key and backup for other key).
                        // 3. A transaction is included into previous incremental snapshot.
                        if (participatingNodes == null)
                            return;

                        procTxCnt.increment();

                        calcTxHash.accept(txRec.nearXidVersion(), participatingNodes);
                    }
                    else if (txRec.state() == TransactionState.ROLLED_BACK) {
                        activeDhtTxs.remove(txRec.writeVersion());
                        txPrimParticipatingNodes.remove(txRec.nearXidVersion());
                    }
                });

                // All active transactions that didn't log COMMITTED or ROLL_BACK records are considered committed.
                // It is possible as incremental snapshot started after transaction left IgniteTxManager#activeTransactions() collection,
                // but completed before the final TxRecord was written.
                for (Map.Entry<GridCacheVersion, Set<Short>> tx: txPrimParticipatingNodes.entrySet())
                    calcTxHash.accept(tx.getKey(), tx.getValue());

                Map<Object, TransactionsHashRecord> txHashRes = nodesTxHash.entrySet().stream()
                    .map(e -> new TransactionsHashRecord(
                        consId,
                        blt.compactIdMapping().get(e.getKey()),
                        e.getValue().hash
                    ))
                    .collect(Collectors.toMap(
                        TransactionsHashRecord::remoteConsistentId,
                        Function.identity()
                    ));

                Map<PartitionKey, PartitionHashRecord> partHashRes = partMap.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new PartitionHashRecord(
                            e.getKey(),
                            false,
                            consId,
                            null,
                            0,
                            null,
                            new VerifyPartitionContext(e.getValue())
                        )
                    ));

                if (log.isInfoEnabled()) {
                    log.info("Verify incremental snapshot procedure finished " +
                        "[snpName=" + snpName + ", incrementIndex=" + incIdx + ", consId=" + consId +
                        ", txCnt=" + procTxCnt.sum() + ", dataEntries=" + procEntriesCnt.sum() +
                        ", walSegments=" + procSegCnt.get() + ']');
                }

                return new IncrementalSnapshotVerificationTaskResult(
                    txHashRes,
                    partHashRes,
                    partiallyCommittedTxs,
                    exceptions);
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** Checks that current baseline topology matches baseline topology of the snapshot. */
        private void checkBaseline(BaselineTopology blt) throws IgniteCheckedException, IOException {
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            SnapshotMetadata meta = snpMgr.readSnapshotMetadata(sft.meta());

            if (!F.eqNotOrdered(blt.consistentIds(), meta.baselineNodes())) {
                throw new IgniteCheckedException("Topologies of snapshot and current cluster are different [snp=" +
                    meta.baselineNodes() + ", current=" + blt.consistentIds() + ']');
            }
        }

        /** @return Collection of snapshotted transactional caches, key is a cache ID. */
        private Map<Integer, StoredCacheData> readTxCachesData() {
            return GridLocalConfigManager.readCachesData(
                    sft.nodeStorage(),
                    ignite.context().marshallerContext().jdkMarshaller(),
                    ignite.configuration())
                .values().stream()
                .filter(data -> data.config().getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL)
                .collect(Collectors.toMap(StoredCacheData::cacheId, Function.identity()));
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

    /** Holder for calculated hashes. */
    public static class HashHolder {
        /** */
        public int hash;

        /** */
        public int verHash;

        /** */
        public void increment(int hash, int verHash) {
            this.hash += hash;
            this.verHash += verHash;
        }
    }
}
