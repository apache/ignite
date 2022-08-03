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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CONSISTENT_CUT_START_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.TX_RECORD;

/**
 * WAL Reader for ConsistentCut tests. Read WAL for specified node and validate it.
 */
class ConsistentCutWalReader {
    /** Compact ID of Ignite node. */
    final short nodeCompactId;

    /** Iterator over WAL archive files. */
    private final WALIterator walIter;

    /** */
    private final IgniteLogger log;

    /** Collection of Consistent Cut states. */
    public final List<NodeConsistentCutState> cuts = new ArrayList<>();

    /** Collection of transactions was run within a test. */
    private final Map<IgniteUuid, Integer> txNearNode;

    /** Ignite nodes description: (nodeIdx -> compactId). */
    private final Map<Integer, Short> txCompactNode;

    /** Order ID of Ignite node. */
    private final Integer nodeIdx;

    /** For test purposes. */
    ConsistentCutWalReader(
        int nodeIdx,
        WALIterator walIter,
        IgniteLogger log,
        Map<IgniteUuid, Integer> txNearNode,
        Map<Integer, Short> txCompactNode
    ) {
        this.walIter = walIter;
        nodeCompactId = txCompactNode.get(nodeIdx);
        this.log = log;

        this.nodeIdx = nodeIdx;
        this.txNearNode = txNearNode;
        this.txCompactNode = txCompactNode;
    }

    /** Reads whole WAL. */
    void read() throws Exception {
        NodeConsistentCutState curCut = new NodeConsistentCutState(1, null, null, null);

        // Use buffer to avoid offset WAL multiple times (before cut, await finish cut, apply inclusions).
        BufferWalIterator it = new BufferWalIterator(walIter);

        while (true) {
            ConsistentCutStartRecord startCutRecord = readBeforeCut(it, curCut);

            // INCOMPLETE - State between the latest completed Consistent Cut and WAL finish.
            if (startCutRecord == null) {
                curCut.ver = NodeConsistentCutState.INCOMPLETE;

                // Skip if there is no committed transaction after the latest Consistent Cut.
                if (!curCut.committedTx.isEmpty())
                    cuts.add(curCut);

                it.close();

                return;
            }

            if (awaitFinishCut(it, curCut)) {
                readCutInclusions(it, curCut);

                // Skip includes in buffer when re-read them from buffer.
                it.skipTxInBuffer(new HashSet<>(curCut.txInclude));

                cuts.add(curCut);

                curCut = new NodeConsistentCutState(curCut.ver + 1, curCut.txExclude, curCut.txInclude, curCut.txParticipations);
            }
            else
                assert false : "Doesn't get expected ConsistentCutFinishRecord. " + curCut.ver;
        }
    }

    /**
     * @param it WAL iterator to read.
     * @param cut Current ConsistencyCut state.
     */
    private ConsistentCutStartRecord readBeforeCut(BufferWalIterator it, NodeConsistentCutState cut) {
        // Re-read all items since previous ConsistentCutStartRecord.
        it.resetBuffer();
        it.mode(BufferWalIterator.BufferedMode.CLEAN);

        if (log.isDebugEnabled())
            dumpBuf(it);

        while (it.hasNext()) {
            WALRecord rec = it.next().getValue();

            if (rec.type() == TX_RECORD)
                handleTxRecord((TxRecord)rec, cut, false);
            if (rec.type() == WALRecord.RecordType.DATA_RECORD_V2)
                handleDataRecord((DataRecord)rec, cut);
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

    /** Read WAL until ConsistentCutFinishRecord achieved. */
    private boolean awaitFinishCut(BufferWalIterator it, NodeConsistentCutState cut) {
        // Store all records between Start and Finish records to re-read them again.
        it.mode(BufferWalIterator.BufferedMode.STORE);

        if (log.isDebugEnabled())
            dumpBuf(it);

        while (it.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> next = it.next();

            WALRecord rec = next.getValue();

            if (rec.type() == CONSISTENT_CUT_FINISH_RECORD) {
                handleFinishConsistentCutRecord((ConsistentCutFinishRecord)rec, cut);

                return true;
            }
            else if (rec.type() == CONSISTENT_CUT_START_RECORD)
                assert false : "Lost FINISH record. Next cut: " + rec;
            else {
                if (rec.type() == TX_RECORD)
                    log("SKIP[" + ((TxRecord)rec).nearXidVersion().asIgniteUuid() + ", " + ((TxRecord)rec).state() + "]");
            }
        }

        return false;
    }

    /** Read WAL to find all transactions to include to specific Consistent Cut. */
    private void readCutInclusions(BufferWalIterator it, NodeConsistentCutState cut) {
        // Re-read all items since StartRecord to find all inclusions.
        it.resetBuffer();
        it.mode(BufferWalIterator.BufferedMode.STORE);

        if (log.isDebugEnabled())
            dumpBuf(it);

        Set<IgniteUuid> include = new HashSet<>(cut.txInclude);

        StringBuilder bld = new StringBuilder("AWAIT INCLUDED " + include + " for CUT=" + cut.ver);

        for (IgniteUuid tx: cut.txInclude) {
            bld.append("\n\tAwait ").append(tx).append(" ").append(cut.txParticipations.get(tx));

            // Skip inclusions that don't participate on this node at all.
            if (cut.txParticipations.get(tx) != null && cut.txParticipations.get(tx).dataParticipationCnt < 0) {
                include.remove(tx);
                cut.txParticipations.remove(tx);

                bld.append(" SKIP AWAIT");
            }

            // Skip inclusions that already committed.
            if (cut.txParticipations.get(tx) == null) {
                if (cut.committedTx.contains(tx) || (cuts.size() > 1 && cuts.get(cuts.size() - 1).committedTx.contains(tx))) {
                    include.remove(tx);

                    bld.append(" SKIP AWAIT");
                }
            }
        }

        log(bld.toString());

        while (it.hasNext() && !include.isEmpty()) {
            IgniteBiTuple<WALPointer, WALRecord> next = it.next();

            WALRecord rec = next.getValue();

            if (rec.type() == WALRecord.RecordType.DATA_RECORD_V2) {
                DataRecord dataRec = (DataRecord)rec;

                IgniteUuid txId = dataRec.writeEntries().get(0).nearXidVersion().asIgniteUuid();

                if (include.contains(txId))
                    handleDataRecord(dataRec, cut);
            }
            else if (rec.type() == TX_RECORD) {
                TxRecord tx = (TxRecord)rec;

                IgniteUuid uid = tx.nearXidVersion().asIgniteUuid();

                if (include.contains(uid)) {
                    if (tx.state() == TransactionState.PREPARED) {
                        handlePreparedTx(tx, cut, false);

                        // Skip inclusions that don't participate on this node at all.
                        if (cut.txParticipations.get(uid).dataParticipationCnt < 0) {
                            include.remove(uid);
                            cut.txParticipations.remove(uid);
                        }
                    }
                    else {
                        handleTxRecord(tx, cut, true);

                        if (!cut.txParticipations.containsKey(uid))
                            include.remove(uid);
                    }
                }
            }
            else if (rec.type() == CONSISTENT_CUT_START_RECORD) {
                ConsistentCutStartRecord r = (ConsistentCutStartRecord)rec;

                log("SKIP START CUT[" + r + "]");
            }
        }
    }

    /** */
    private void handleDataRecord(DataRecord dataRec, NodeConsistentCutState cut) {
        for (DataEntry e: dataRec.writeEntries()) {
            IgniteUuid txId = e.nearXidVersion().asIgniteUuid();

            assert cut.txParticipations.containsKey(txId) : txId;

            cut.txParticipations.get(txId).start();
        }
    }

    /** */
    private void handleTxRecord(TxRecord tx, NodeConsistentCutState cut, boolean awaited) {
        IgniteUuid uid = tx.nearXidVersion().asIgniteUuid();

        if (tx.state() == TransactionState.COMMITTED) {
            if (cut.addFinishedTx(uid))
                logCommittedTxRecord(tx, cut);
            else
                log("--- TX[id=" + uid + ", state=" + tx.state() + "]");
        }
        else if (!awaited && tx.state() == TransactionState.PREPARED) {
            if (cut.txExclude != null && cut.txExclude.contains(uid))
                log("--- TX[id=" + uid + ", state=" + tx.state());
            else
                handlePreparedTx(tx, cut, false);
        }
    }

    /** */
    private void handlePreparedTx(TxRecord tx, NodeConsistentCutState cut, boolean assertOnePhaseCommit) {
        // Here we calculate how many records we need to wait for a particular transaction.
        Map<Short, Collection<Short>> nodes = tx.participatingNodes();

        // 1 primary node and at most 1 backup. `GridNearTxPrepareFutureAdapter#checkOnePhase`.
        boolean onePhaseCommit = nodes.size() == 1 && nodes.values().iterator().next().size() <= 1;

        assert !assertOnePhaseCommit || onePhaseCommit : "tx=" + tx.nearXidVersion().asIgniteUuid() + ", part=" + tx.participatingNodes();

        logTxParticipations(tx);

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

        // `GridNearTxLocal` writes PREPARED and COMMITTED to WAL even if it doesn't hold any data. It affects case when
        // node participates as near and backup (or just near). In such cases there is additional meaningless COMMITTED
        // record in WAL.
        boolean maybeNear = false;

        int participationCnt;

        // Fast commit - no writes to data.
        if (nodes.isEmpty())
            participationCnt = 0;
        // This case: this node is an originated node, and it didn't participate in TX nor as primary node, nor as backup node.
        // Then it may trigger one-phase process of txs committment on this node.
        else if (primaryParticipate + backupParticipate == 0)
            participationCnt = Integer.MIN_VALUE;
        else
        // This case: ordinary node participation. It could be originated or not, it could participate as primary or backup
        // multiple times.
            participationCnt = primaryParticipate + backupParticipate;

        if (primaryParticipate == 0)
            maybeNear = true;

        cut.initNodeTxParticipations(tx.nearXidVersion().asIgniteUuid(), participationCnt, onePhaseCommit, maybeNear);

        logPreparedTxRecord(tx, cut);
    }

    /** */
    private ConsistentCutStartRecord handleStartConsistentCutRecord(ConsistentCutStartRecord rec, NodeConsistentCutState cut) {
        log("START " + cut.ver + " CUT[" + rec + "], state = " + cut);

        assert cut.ver == rec.version().version() : cut.ver + " " + rec.version().version();

        return rec;
    }

    /** */
    private void handleFinishConsistentCutRecord(ConsistentCutFinishRecord rec, NodeConsistentCutState cut) {
        Set<IgniteUuid> include = rec.before().stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toSet());

        Set<IgniteUuid> exclude = rec.after().stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toSet());

        cut.includeToCut(include);
        cut.excludeFromCut(exclude);

        log("FINISH " + cut.ver + " CUT[" + rec + "], state = " + cut);
    }

    /** */
    private void dumpBuf(BufferWalIterator bufWalIt) {
        List<IgniteBiTuple<WALPointer, WALRecord>> buf = bufWalIt.buffer();

        if (buf == null)
            return;

        for (IgniteBiTuple<WALPointer, WALRecord> r: buf) {
            WALRecord rec = r.getValue();

            if (rec instanceof TxRecord) {
                TxRecord tx = (TxRecord)rec;

                log("\tBUFF TX: " + tx.nearXidVersion().asIgniteUuid() + " " + tx.state());
            }
            else if (rec instanceof ConsistentCutStartRecord) {
                ConsistentCutStartRecord cut = (ConsistentCutStartRecord)rec;

                log("\tBUFF CUT: " + cut);
            }
            else if (rec instanceof ConsistentCutFinishRecord) {
                ConsistentCutFinishRecord cut = (ConsistentCutFinishRecord)rec;

                log("\tBUFF CUT: " + cut);
            }
        }

        if (bufWalIt.skipTx() != null)
            log("\tSKIP: " + bufWalIt.skipTx());
    }

    /** Describes Consistent Cut state basing on reading WAL. */
    static class NodeConsistentCutState {
        /** State between the latest completed Consistent Cut and WAL finish. */
        static final int INCOMPLETE = Integer.MAX_VALUE;

        /** Consistent Cut version. */
        @GridToStringInclude
        long ver;

        /**
         * Committed transactions.
         */
        @GridToStringInclude
        final Set<IgniteUuid> committedTx = new HashSet<>();

        /** Transactions to exclude from this state while reading WAL. */
        @GridToStringInclude
        Set<IgniteUuid> txExclude;

        /** Transactions to include to this state while reading WAL. */
        @GridToStringInclude
        Set<IgniteUuid> txInclude;

        /**
         * For every transaction there is a pair <Boolean, Integer> - whether commits started, and count of commits to await
         * on this node. Multiple commits if node participates in transaction multiple times (as primary, as backup).
         */
        @GridToStringInclude
        final Map<IgniteUuid, NodeTxParticipation> txParticipations;

        /**
         * @param exclude List of TX to exclude from this CUT, because they are part of previous CUT.
         * @param txParticipations Participations collection inherited from previous states.
         */
        NodeConsistentCutState(
            long ver,
            @Nullable Set<IgniteUuid> include,
            @Nullable Set<IgniteUuid> exclude,
            @Nullable Map<IgniteUuid, NodeTxParticipation> txParticipations
        ) {
            this.ver = ver;

            if (include != null)
                committedTx.addAll(include);

            txExclude = exclude == null ? new HashSet<>() : new HashSet<>(exclude);

            this.txParticipations = txParticipations == null ? new HashMap<>() : new HashMap<>(txParticipations);
        }

        /** */
        public boolean addFinishedTx(IgniteUuid txId) {
            if (txExclude != null && txExclude.contains(txId))
                return false;

            if (txParticipate(txId)) {
                committedTx.add(txId);

                return true;
            }

            return false;
        }

        /** Includes set of transactions to this state. */
        public void includeToCut(Set<IgniteUuid> txs) {
            txInclude = new HashSet<>(txs);
        }

        /** Includes set of transactions to this state. */
        public void excludeFromCut(Set<IgniteUuid> txs) {
            // Clear exclusions from previous CUT.
            txExclude.clear();

            for (IgniteUuid tx: txs) {
                committedTx.remove(tx);
                txExclude.add(tx);
            }
        }

        /**
         * Set amount of participations of this node in specified transaction.
         *
         * @param txId Transaction ID.
         * @param participations Times this node participate in transaction as node (primary + backups)
         * @param onePhaseCommit {@code true} if this transaction is 1PC, {@code false} for 2PC.
         * @param maybeNear {@code true} if this transaction may have additional participation as near node.
         */
        public void initNodeTxParticipations(IgniteUuid txId, int participations, boolean onePhaseCommit, boolean maybeNear) {
            if (txExclude != null && txExclude.contains(txId))
                return;

            if (!txParticipations.containsKey(txId))
                txParticipations.put(txId, new NodeTxParticipation(participations, maybeNear));
            // For one-phase commits it's possible to get PREPARED after COMMITTED, then actual participate cnt can be different.
            else
                assert onePhaseCommit || txParticipations.get(txId).dataParticipationCnt == participations : txId;
        }

        /**
         * Handles participation of this node in specified transaction.
         *
         * @return {@code true} if this node participated in this tx, otherwise {@code false}.
         */
        private boolean txParticipate(IgniteUuid txId) {
            // Artifacts from previous cut states.
            if (!txParticipations.containsKey(txId))
                return false;

            // Found how many times this transaction will be committed on this node (due to backups).
            // Set the flag to `false` because it has prepared only.
            NodeTxParticipation p = txParticipations.get(txId);

            // For 2PC case when node participate as near and backup. Then it will have first COMMITTED record without data entries.
            if (!p.started) {
                assert p.maybeNear : txId + " " + p;

                return false;
            }

            boolean isParticipate = p.participate();

            if (p.dataParticipationCnt <= 0)
                txParticipations.remove(txId);

            return isParticipate;
        }

        /** */
        public boolean checkWriteEntry(IgniteUuid uid) {
            if (txExclude != null && txExclude.contains(uid))
                return true;

            return txParticipations.containsKey(uid);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(NodeConsistentCutState.class, this);
        }

        /**
         * Mutable state of how specific node participates in transaction during parsing its WAL.
         *
         * A participation means WAL of this node have COMMITTED record (one record per one participation).
         */
        private static class NodeTxParticipation {
            /** How many times this node participates in transaction as data node (backup or primary). */
            @GridToStringInclude
            int dataParticipationCnt;

            /** Whether this node may have an additional participation as near node. */
            @GridToStringInclude
            final boolean maybeNear;

            /** It's started when first DataEntry related to this transaction appeared in WAL. */
            @GridToStringInclude
            boolean started;

            /** */
            NodeTxParticipation(int dataParticipationCnt, boolean maybeNear) {
                this.dataParticipationCnt = dataParticipationCnt;
                this.maybeNear = maybeNear;
            }

            /**
             * @return {@code true} if node participates in transaction.
             */
            boolean participate() {
                // This node doesn't participate in transaction.
                if (dataParticipationCnt == Integer.MIN_VALUE)
                    return false;

                dataParticipationCnt--;

                return true;
            }

            /** */
            void start() {
                started = true;
            }

            /** {@inheritDoc} */
            @Override public String toString() {
                return S.toString(NodeTxParticipation.class, this);
            }
        }
    }

    /** */
    private void log(String msg) {
        if (log.isDebugEnabled())
            log.debug(msg);
    }

    /** */
    private void logCommittedTxRecord(TxRecord tx, NodeConsistentCutState cut) {
        IgniteUuid uid = tx.nearXidVersion().asIgniteUuid();

        log("TX[id=" + uid + ", state=" + tx.state() + ", cut=" + cut.ver
            + ", origNodeId=" + txNearNode.get(uid) + ", nodeId=" + nodeIdx + ", participations="
            + cut.txParticipations.get(uid) + "]");
    }

    /** */
    private void logTxParticipations(TxRecord tx) {
        IgniteUuid uid = tx.nearXidVersion().asIgniteUuid();

        Map<Short, Collection<Short>> nodes = tx.participatingNodes();

        Short origCompactId = txCompactNode.get(txNearNode.get(uid));

        // -1 means client node.
        if (origCompactId == null)
            origCompactId = -1;

        log("PART[txId=" + uid + ", partNodes=" + nodes + ", origNodeCompactId=" + origCompactId + "]");
    }

    /** */
    private void logPreparedTxRecord(TxRecord tx, NodeConsistentCutState cut) {
        IgniteUuid uid = tx.nearXidVersion().asIgniteUuid();

        int nearNodeId = txNearNode.get(uid);

        short origCompactId = -1;

        if (txCompactNode.containsKey(nearNodeId))
            origCompactId = txCompactNode.get(nearNodeId);

        log("TX[id=" + uid + ", state=" + tx.state() + ", cut=" + cut.ver +
            ", origNodeId=" + txNearNode.get(uid) + ", origCompactId=" + origCompactId +
            ", nodeId=" + nodeIdx + ", compactId=" + nodeCompactId +
            ", participations=" + cut.txParticipations.get(uid) + "]");
    }
}
