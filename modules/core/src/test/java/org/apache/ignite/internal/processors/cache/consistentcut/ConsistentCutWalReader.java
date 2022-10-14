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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
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

    /** Read WAL and fills {@link #cuts} with read Consistent Cuts. */
    void read() {
        NodeConsistentCutState curCut = new NodeConsistentCutState(0, new HashMap<>());

        // Use buffer to avoid offset WAL multiple times (before cut, await finish cut, apply inclusions).
        BufferWalIterator it = new BufferWalIterator(walIter);

        while (true) {
            boolean finished = readFinishCutRecord(it, curCut);

            readBeforeFinishCut(it, curCut);

            cuts.add(curCut);

            curCut = curCut.next();

            if (!finished)
                return;
        }
    }

    /**
     * Read WAL until {@link ConsistentCutFinishRecord} achieved.
     *
     * @return {@code true} if {@link ConsistentCutFinishRecord} found, otherwise {@code false}.
     */
    private boolean readFinishCutRecord(BufferWalIterator it, NodeConsistentCutState cut) {
        // Store all records between Start and Finish records to re-read them again.
        it.mode(BufferWalIterator.BufferedMode.STORE);
        dumpBuf(it);

        boolean started = false;

        while (it.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> next = it.next();

            WALRecord rec = next.getValue();

            if (rec.type() == CONSISTENT_CUT_FINISH_RECORD) {
                assert started : "Lost START record. Next cut: " + rec;

                handleFinishConsistentCutRecord((ConsistentCutFinishRecord)rec, cut);

                return true;
            }
            else if (rec.type() == CONSISTENT_CUT_START_RECORD) {
                assert !started : "Lost FINISH record. Next cut: " + rec;

                started = true;
            }
        }

        return false;
    }

    /**
     * @param it WAL iterator to read.
     * @param cut Current ConsistencyCut state.
     */
    private void readBeforeFinishCut(BufferWalIterator it, NodeConsistentCutState cut) {
        // Re-read all items since previous ConsistentCutStartRecord.
        it.resetBuffer();
        it.mode(BufferWalIterator.BufferedMode.CLEAN);

        boolean beforeStartRec = true;

        while (it.hasNext()) {
            WALRecord rec = it.next().getValue();

            if (rec.type() == TX_RECORD)
                handleTxRecord((TxRecord)rec, cut, beforeStartRec);
            if (rec.type() == WALRecord.RecordType.DATA_RECORD_V2)
                handleDataRecord((DataRecord)rec, cut, beforeStartRec);
            else if (rec.type() == CONSISTENT_CUT_START_RECORD) {
                assert beforeStartRec : "Lost Finish record";

                ConsistentCutStartRecord r = (ConsistentCutStartRecord)rec;

                handleStartConsistentCutRecord(r, cut);

                beforeStartRec = false;
            }
            else if (rec.type() == CONSISTENT_CUT_FINISH_RECORD) {
                assert !beforeStartRec : "Lost Start record";

                log("FINISH " + cut.num + " CUT[" + rec + "], state = " + cut);

                return;
            }
        }
    }

    /** */
    private void handleDataRecord(DataRecord dataRec, NodeConsistentCutState cut, boolean beforeStartRec) {
        IgniteUuid xid = null;

        for (DataEntry e: dataRec.writeEntries()) {
            IgniteUuid id = e.nearXidVersion().asIgniteUuid();

            assert xid == null || xid.equals(id) : xid + " " + id;

            xid = id;
        }

        cut = cut.cutState(xid, beforeStartRec);

        log("ENTRY TX[" + xid + "], cut=" + cut.num);

        cut.addWriteEntry(xid);
    }

    /** */
    private void handleTxRecord(TxRecord tx, NodeConsistentCutState cut, boolean beforeStartRec) {
        IgniteUuid xid = tx.nearXidVersion().asIgniteUuid();

        cut = cut.cutState(xid, beforeStartRec);

        if (tx.state() == TransactionState.COMMITTED) {
            cut.commitTx(xid);

            logCommittedTxRecord(tx, cut);
        }
        else if (tx.state() == TransactionState.PREPARED)
            handlePreparedTx(tx, cut, false);
    }

    /** */
    private void handlePreparedTx(TxRecord tx, NodeConsistentCutState cut, boolean assertOnePhaseCommit) {
        // Here we calculate how many records we need to wait for a particular transaction.
        Map<Short, Collection<Short>> nodes = tx.participatingNodes();

        // 1 primary node and at mostf 1 backup. `GridNearTxPrepareFutureAdapter#checkOnePhase`.
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
    private void handleStartConsistentCutRecord(ConsistentCutStartRecord rec, NodeConsistentCutState cut) {
        log("START " + cut.num + " CUT[" + rec + "], state = " + cut);

        cut.ts = rec.marker().version();
    }

    /** */
    private void handleFinishConsistentCutRecord(ConsistentCutFinishRecord rec, NodeConsistentCutState cut) {
        Set<IgniteUuid> include = rec.before().stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toSet());

        Set<IgniteUuid> exclude = rec.after().stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toSet());

        cut.finish(include, exclude);
    }

    /** */
    private void dumpBuf(BufferWalIterator bufWalIt) {
        if (!log.isDebugEnabled())
            return;

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
        /** */
        @GridToStringExclude
        private NodeConsistentCutState next;

        /** Consistent Cut timestamp. Set after reach {@link ConsistentCutStartRecord}. */
        @GridToStringInclude
        Long ts;

        /** If {@link ConsistentCutFinishRecord} is written for this cut, than this cut is completed. */
        @GridToStringInclude
        boolean completed;

        /** Consistent Cut number. */
        @GridToStringInclude
        final int num;

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
        @GridToStringExclude
        final Map<IgniteUuid, NodeTxParticipation> txParticipations;

        /** */
        NodeConsistentCutState(int num, @Nullable Map<IgniteUuid, NodeTxParticipation> part) {
            this.num = num;

            // Share link between multiple cuts.
            txParticipations = part;
        }

        /** */
        public boolean commitTx(IgniteUuid xid) {
            return txParticipate(xid);
        }

        /** */
        void finish(Set<IgniteUuid> include, Set<IgniteUuid> exclude) {
            txInclude = new HashSet<>(include);
            txExclude = new HashSet<>(exclude);

            completed = true;
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
        private boolean txParticipate(IgniteUuid xid) {
            // Artifacts from previous cut states.
            if (!txParticipations.containsKey(xid))
                return false;

            // Found how many times this transaction will be committed on this node (due to backups).
            // Set the flag to `false` because it has prepared only.
            NodeTxParticipation p = txParticipations.get(xid);

            // For 2PC case when node participate as near and backup. Then it will have first COMMITTED record without data entries.
            if (!p.started) {
                assert p.maybeNear : xid + " " + p;

                return false;
            }

            boolean isParticipate = p.participate();

            if (p.dataParticipationCnt <= 0)
                txParticipations.remove(xid);

            return isParticipate;
        }

        /**
         * @param xid Transaction near ID.
         */
        public void addWriteEntry(IgniteUuid xid) {
            assert txParticipations.containsKey(xid) : xid;

            txParticipations.get(xid).start();

            committedTx.add(xid);
        }

        /**
         * @return {@code this} if this transaction belongs to current cut, otherwise {@link #next()}.
         */
        public NodeConsistentCutState cutState(IgniteUuid xid, boolean beforeStartRec) {
            if (!completed)
                return this;

            boolean cur = beforeStartRec ? !txExclude.contains(xid) : txInclude.contains(xid);

            return cur ? this : next();
        }

        /** */
        private NodeConsistentCutState next() {
            if (next == null)
                next = new NodeConsistentCutState(num + 1, txParticipations);

            return next;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(NodeConsistentCutState.class, this);
        }

        /**
         * Mutable state of how specific node participates in transaction during parsing its WAL.
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
            log.info(msg);
    }

    /** */
    private void logCommittedTxRecord(TxRecord tx, NodeConsistentCutState cut) {
        IgniteUuid xid = tx.nearXidVersion().asIgniteUuid();

        log("TX[id=" + xid + ", state=" + tx.state() + ", cut=" + cut.num
            + ", origNodeId=" + txNearNode.get(xid) + ", nodeId=" + nodeIdx + ", participations="
            + cut.txParticipations.get(xid) + "]");
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

        log("TX[id=" + uid + ", state=" + tx.state() + ", cut=" + cut.num +
            ", origNodeId=" + txNearNode.get(uid) + ", origCompactId=" + origCompactId +
            ", nodeId=" + nodeIdx + ", compactId=" + nodeCompactId +
            ", participations=" + cut.txParticipations.get(uid) + "]");
    }
}
