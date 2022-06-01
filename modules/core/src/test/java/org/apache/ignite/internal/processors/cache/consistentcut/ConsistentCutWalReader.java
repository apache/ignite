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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_START_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.TX_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.genNewStyleSubfolderName;

/**
 * WAL Reader for ConsistentCut tests. Read WAL for specified node and validate it.
 */
public class ConsistentCutWalReader {
    /** */
    private final IgniteLogger log;

    /** */
    private final Serializable consistentId;

    /** */
    private final int nodeIdx;

    /** */
    private final short nodeCompactId;

    /** */
    public final List<NodeConsistentCutState> cuts = new ArrayList<>();

    /** Initied cut version. Counter -> CutVersion */
    private final Map<Integer, Long> cutVers = new HashMap<Integer, Long>() {
        {
            put(1, null);
        }
    };

    /** */
    private int curCutVer = 1;

    /** */
    private final Map<IgniteUuid, Integer> txOrigNode;

    /** */
    private final Map<Integer, T2<Serializable, Short>> txCompactNode;

    /** */
    ConsistentCutWalReader(
        int nodeIdx,
        IgniteLogger log,
        Map<IgniteUuid, Integer> txOrigNode,
        Map<Integer, T2<Serializable, Short>> txCompactNode
    ) {
        consistentId = txCompactNode.get(nodeIdx).get1();
        nodeCompactId = txCompactNode.get(nodeIdx).get2();
        this.nodeIdx = nodeIdx;
        this.log = log;
        this.txOrigNode = txOrigNode;
        this.txCompactNode = txCompactNode;
    }

    /** */
    public void read() throws Exception {
        WALPointer from = null;

        NodeConsistentCutState curCut = new NodeConsistentCutState(null, null);

        while (true) {
            WALIterator it = walIter(from);

            ConsistentCutStartRecord startCutRecord = readBeforeCut(it, curCut);

            if (startCutRecord == null) {
                curCut.ver = NodeConsistentCutState.INCOMPLETE;

                cuts.add(curCut);

                it.close();

                return;
            }

            WALPointer startCutPtr = it.lastRead().orElse(null);

            assert startCutPtr != null;

            awaitFinishCut(it, curCut);

            assert startCutPtr.compareTo(it.lastRead().orElse(null)) < 0;

            from = startCutPtr.next();

            it.close();

            it = walIter(from);

            readCutInclusions(it, curCut);

            cuts.add(curCut);

            curCut = new NodeConsistentCutState(curCut.txInclude, curCut.txParticipations);
        }
    }

    /**
     * @param it WAL iterator to read.
     * @param cut Current ConsistencyCut state.
     */
    private ConsistentCutStartRecord readBeforeCut(WALIterator it, NodeConsistentCutState cut) {
        while (it.hasNext()) {
            WALRecord rec = it.next().getValue();

            if (rec.type() == WALRecord.RecordType.DATA_RECORD_V2)
                handleDataRecord((DataRecord)rec, cut);
            else if (rec.type() == WALRecord.RecordType.TX_RECORD)
                handleTxRecord((TxRecord)rec, cut, false);
            else if (rec.type() == CONSISTENT_CUT_START_RECORD) {
                ConsistentCutStartRecord r = (ConsistentCutStartRecord)rec;

                r = handleStartConsistentCutRecord(r, cut);

                // Re-read second cut record with exclusion.
                if (r == null)
                    continue;

                return r;
            }
        }

        // Reach end of WAL here. No ConsistentCut more.
        return null;
    }

    /** */
    private void awaitFinishCut(WALIterator it, NodeConsistentCutState cut) {
        while (it.hasNext()) {
            WALRecord rec = it.next().getValue();

            if (rec.type() == CONSISTENT_CUT_FINISH_RECORD) {
                handleFinishConsistentCutRecord((ConsistentCutFinishRecord)rec, cut);

                return;
            }
            else if (rec.type() == TX_RECORD)
                System.out.println("SKIP[" + ((TxRecord)rec).nearXidVersion().asIgniteUuid() + ", " + ((TxRecord)rec).state() + "]");
        }
    }

    /** */
    private void readCutInclusions(WALIterator it, NodeConsistentCutState cut) {
        Set<IgniteUuid> include = new HashSet<>(cut.txInclude);

        StringBuilder bld = new StringBuilder("AWAIT INCLUDED " + include + " for CUT=" + cut.ver);

        for (IgniteUuid tx: cut.txInclude) {
            bld.append("\n\tAwait ").append(tx).append(" ").append(cut.txParticipations.get(tx));

            // Skip inclusions that don't participate on this node at all.
            if (cut.txParticipations.get(tx) != null && cut.txParticipations.get(tx).get2() < 0) {
                include.remove(tx);
                cut.txParticipations.remove(tx);

                bld.append(" SKIP AWAIT");
            }

            // Skip inclusions that already committed.
            if (cut.txParticipations.get(tx) == null && cut.committedTx.contains(tx)) {
                include.remove(tx);

                bld.append(" SKIP AWAIT");
            }
        }

        System.out.println(bld);

        while (it.hasNext() && !include.isEmpty()) {
            WALRecord rec = it.next().getValue();

            if (rec.type() == WALRecord.RecordType.DATA_RECORD_V2) {
                DataRecord r = (DataRecord)rec;

                IgniteUuid tx = r.writeEntries().get(0).nearXidVersion().asIgniteUuid();

                if (include.contains(tx))
                    handleDataRecord(r, cut);
            }
            if (rec.type() == WALRecord.RecordType.TX_RECORD) {
                TxRecord tx = (TxRecord)rec;

                IgniteUuid uid = txId(tx);

                if (include.contains(uid)) {
                    if (tx.state() == TransactionState.PREPARED) {
                        handlePreparedTx(uid, tx, cut, false);

                        // Skip inclusions that don't participate on this node at all.
                        if (cut.txParticipations.get(uid).get2() < 0) {
                            include.remove(uid);
                            cut.txParticipations.remove(uid);
                        }
                    }
                    else {
                        assert tx.state() == TransactionState.COMMITTED : uid + " " + tx.state();

                        handleTxRecord(tx, cut, true);

                        if (!cut.txParticipations.containsKey(uid))
                            include.remove(uid);
                    }
                }
                else
                    System.out.println("SKIP EXCLUDED TX[" + tx.nearXidVersion().asIgniteUuid() + " " + tx.state() + "]");
            }
            else if (rec.type() == CONSISTENT_CUT_START_RECORD) {
                ConsistentCutStartRecord r = (ConsistentCutStartRecord)rec;

                assert cut.ver == r.version();

                System.out.println("SKIP CUT[" + r + "]");
            }
        }
    }

    /** */
    private void handleDataRecord(DataRecord rec, NodeConsistentCutState cut) {
        for (DataEntry de: rec.writeEntries()) {
            IgniteUuid txId = de.nearXidVersion().asIgniteUuid();

            assert cut.checkWriteEntry(txId) : "There are writeEntries with no transaction: " + txId;
        }
    }

    /** */
    private void handleTxRecord(TxRecord tx, NodeConsistentCutState cut, boolean awaited) {
        IgniteUuid uid = txId(tx);

        if (tx.state() == TransactionState.COMMITTED) { // || tx.state() == TransactionState.ROLLED_BACK) {
            String cutVer = awaited ? "incl in " + (curCutVer - 1) : String.valueOf(curCutVer);

            if (cut.addCommittedTx(uid)) {
                System.out.println("TX[id=" + uid + ", state=" + tx.state() + ", cut=" + cutVer
                    + ", origNodeId=" + txOrigNode.get(uid) + ", nodeId=" + nodeIdx + ", participations="
                    + cut.txParticipations.get(uid) + "]");
            }
        }
        else if (!awaited && tx.state() == TransactionState.PREPARED)
            handlePreparedTx(uid, tx, cut, false);
    }

    /** */
    private void handlePreparedTx(IgniteUuid uid, TxRecord tx, NodeConsistentCutState cut, boolean assertOnePhaseCommit) {
        // Here we calculate how many records we need to wait for a particular transaction.
        Map<Short, Collection<Short>> nodes = tx.participatingNodes();

        // 1 primary node and at most 1 backup. `GridNearTxPrepareFutureAdapter#checkOnePhase`.
        boolean onePhaseCommit = nodes.size() == 1 && nodes.values().iterator().next().size() <= 1;

        assert !assertOnePhaseCommit || onePhaseCommit : "tx=" + tx.nearXidVersion().asIgniteUuid() + ", part=" + tx.participatingNodes();

        T2<Serializable, Short> nearNodeInfo = txCompactNode.get(txOrigNode.get(uid));

        // -1 means client node.
        short origCompactId = -1;

        if (nearNodeInfo != null)
            origCompactId = nearNodeInfo.get2();

        System.out.println("PART[txId=" + uid + ", partNodes=" + nodes + ", origNodeCompactId=" + origCompactId + "]");

        int backupParticipate = 0;
        int primaryParticipate = 0;

        for (Map.Entry<Short, Collection<Short>> e: nodes.entrySet()) {
            if (e.getKey().equals(nodeCompactId))
                primaryParticipate++;
            else if (e.getValue().contains(nodeCompactId))
                backupParticipate++;
                // Short.MAX_VALUE is shortcat. It shows that every node participates in tx as backup.
                // See `org.apache.ignite.internal.managers.discovery.ConsistentIdMapper`.
            else if (e.getValue().size() == 1 && e.getValue().contains(Short.MAX_VALUE))
                backupParticipate++;
        }

        int participationCnt;

        // Fast commit - no writes to data.
        if (nodes.isEmpty())
            participationCnt = 0;
        // This case: this node is an originated node, and it didn't participate in TX nor as primary node, nor as backup node.
        // Then it may trigger one-phase process of txs committment on this node.
        else if (primaryParticipate + backupParticipate == 0)
            participationCnt = Integer.MIN_VALUE;
        // This case: this node is an originated node, and it participated only as backup node. Additional participation
        // is tx's coordination.
        else if (primaryParticipate == 0 && origCompactId == nodeCompactId)
            participationCnt = backupParticipate + 1;
        else
        // This case: ordinary node participation. It could be originated or not, it could participate as primary or backup
        // multiple times.
            participationCnt = primaryParticipate + backupParticipate;

        cut.initNodeTxParticipations(uid, participationCnt, onePhaseCommit);

        System.out.println("TX[id=" + uid + ", state=" + tx.state() + ", cut=" + curCutVer +
            ", origNodeId=" + txOrigNode.get(uid) + ", origCompactId=" + origCompactId +
            ", nodeId=" + nodeIdx + ", compactId=" + nodeCompactId +
            ", participations=" + cut.txParticipations.get(uid) + "]");
    }

    /** */
    private ConsistentCutStartRecord handleStartConsistentCutRecord(ConsistentCutStartRecord rec, NodeConsistentCutState cut) {
        System.out.println("START " + curCutVer + " CUT[" + rec + "]");

        cutVers.put(curCutVer, rec.version());

        ++curCutVer;

        cut.ver = rec.version();

        Set<IgniteUuid> include = rec.include().stream()
            .map(GridCacheVersion::asIgniteUuid)
            // Some transactions that included to CUT may be already committed (partially or fully) before this CUT.
            // Skip if it is fully committed.
            .filter(tx -> {
                if (cut.committedTx.contains(tx))
                    return cut.txParticipations.containsKey(tx);

                return true;
            })
            .collect(Collectors.toSet());

        assert cut.txInclude == null;

        cut.txInclude(include);

        return rec;
    }

    /** */
    private void handleFinishConsistentCutRecord(ConsistentCutFinishRecord rec, NodeConsistentCutState cut) {
        System.out.println("FINISH " + curCutVer + " CUT[" + rec + "]");

        for (GridCacheVersion includedTx: rec.include()) {
            IgniteUuid tx = includedTx.asIgniteUuid();

            cut.includeToCut(tx);
        }
    }

    /** */
    private WALIterator walIter(WALPointer from) throws Exception {
        String workDir = U.defaultWorkDirectory();

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        String subfolderName = genNewStyleSubfolderName(nodeIdx, (UUID)consistentId);

        File archive = U.resolveWorkDirectory(workDir, "db/wal/archive/" + subfolderName, false);

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(archive);

        if (from != null)
            params.from(from);

        return factory.iterator(params);
    }

    /** */
    private IgniteUuid txId(TxRecord tx) {
        return tx.nearXidVersion().asIgniteUuid();
    }

    /** */
    public static class NodeConsistentCutState {
        /** */
        public static final int INCOMPLETE = Integer.MAX_VALUE;

        /** */
        public long ver;

        /**
         * Committed transactions. Doesn't include tx that committed with one-phase algorithm.
         */
        public final List<IgniteUuid> committedTx = new ArrayList<>();

        /** */
        private final Set<IgniteUuid> txExclude;

        /** */
        private Set<IgniteUuid> txInclude;

        /** */
        // <Boolean, Integer> - whether commits started, and count of commits to await.
        private final Map<IgniteUuid, T2<Boolean, Integer>> txParticipations;

        /**
         * @param exclude List of TX to exclude from this CUT, because they are part of previous CUT.
         */
        NodeConsistentCutState(
            @Nullable Set<IgniteUuid> exclude,
            @Nullable Map<IgniteUuid, T2<Boolean, Integer>> txParticipations
        ) {
            txExclude = exclude == null ? null : new HashSet<>(exclude);

            this.txParticipations = txParticipations == null ? new HashMap<>() : new HashMap<>(txParticipations);
        }

        /** */
        public boolean addCommittedTx(IgniteUuid txId) {
            if (txExclude != null && txExclude.contains(txId))
                return false;

            if (txParticipate(txId)) {
                committedTx.add(txId);

                return true;
            }

            return false;
        }

        /** */
        public void txInclude(Set<IgniteUuid> txs) {
            txInclude = new HashSet<>(txs);
        }

        /** */
        public void includeToCut(IgniteUuid tx) {
            txInclude.add(tx);
        }

        /**
         * @param txId Transaction ID.
         * @param participations Times this node participate in transaction as node (primary + backups)
         */
        public void initNodeTxParticipations(IgniteUuid txId, int participations, boolean onePhaseCommit) {
            if (txExclude != null && txExclude.contains(txId))
                return;

            if (!txParticipations.containsKey(txId))
                txParticipations.put(txId, new T2<>(false, participations));
            // For one-phase commits it's possible to get PREPARED after COMMITTED, then actual participate cnt can be different.
            else
                assert onePhaseCommit || txParticipations.get(txId).get2().equals(participations) : txId;
        }

        /**
         * @return {@code true} if this node participated in this tx, otherwise {@code false}.
         */
        private boolean txParticipate(IgniteUuid txId) {
            // Found how many times this transaction will be committed on this node (due to backups).
            // Set the flag to `false` because it has prepared only.

            assert txParticipations.containsKey(txId);

            T2<Boolean, Integer> p = txParticipations.get(txId);

            // This node doesn't participate in transaction.
            if (p.get2() == Integer.MIN_VALUE) {
                txParticipations.remove(txId);

                return false;
            }
            // Fast commit - no writes, node participate only as an originated node.
            else if (p.get2() == 0) {
                txParticipations.remove(txId);

                return true;
            }

            // Commit once at this node. Wait for next commit (backup), or finish this tx with removing from participations.
            int np = p.get2() - 1;

            assert np >= 0;

            if (np == 0)
                txParticipations.remove(txId);
            else
                txParticipations.put(txId, new T2<>(true, np));

            return true;
        }

        /**
         * @return {@code true} if
         */
        public boolean checkWriteEntry(IgniteUuid uid) {
            if (txExclude != null && txExclude.contains(uid))
                return true;

            return txParticipations.containsKey(uid);
        }
    }
}
