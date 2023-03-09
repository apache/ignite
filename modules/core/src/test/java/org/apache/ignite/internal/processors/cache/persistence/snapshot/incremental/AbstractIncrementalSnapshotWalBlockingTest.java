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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionState;

/** */
public abstract class AbstractIncrementalSnapshotWalBlockingTest extends AbstractIncrementalSnapshotBlockingTest {
    /** */
    protected static TransactionState txBlkState;

    /** Initialize latches for test cases with blocking WAL tx states. */
    protected final void initWALCase(TransactionState txState, BlkNodeType txBlkNode, BlkSnpType snpBlkType, BlkNodeType snpBlkNode) {
        txBlkState = txState;
        txBlkNodeType = txBlkNode;

        AbstractIncrementalSnapshotBlockingTest.snpBlkType = snpBlkType;
        snpBlkNodeType = snpBlkNode;
    }

    /** */
    @Override protected void runCase(
        TransactionTestCase testCase,
        int nearNodeIdx,
        TransactionConcurrency txConcurrency
    ) throws Exception {
        int txBlkNodeIdx = blkNodeIndex(nearNodeIdx, txBlkNodeType, testCase);

        int snpBlkNodeId = -1;

        if (snpBlkType != BlkSnpType.NONE)
            snpBlkNodeId = blkNodeIndex(nearNodeIdx, snpBlkNodeType, testCase);

        // Skip cases with blocking WAL on clients (no WAL actually)
        if (txBlkNodeIdx == nodes())
            return;

        log.info("START CASE " + caseNum + ". Data=" + testCase + ", nearNodeIdx=" + nearNodeIdx);

        run(() -> tx(nearNodeIdx, testCase, txConcurrency), txBlkNodeIdx, snpBlkNodeId);
    }

    /** */
    @Override protected void blockTx(IgniteEx blkNode) {
        BlockingWALManager.walMgr(blkNode).block(WALRecord.RecordType.TX_RECORD,
            (rec) -> ((TxRecord)rec).state() == txBlkState);
    }

    /** */
    @Override protected void awaitTxBlocked(IgniteEx blkNode) {
        BlockingWALManager.walMgr(blkNode).awaitBlocked(WALRecord.RecordType.TX_RECORD);
    }

    /** */
    @Override protected void unblockTx(IgniteEx blkNode) {
        BlockingWALManager.walMgr(blkNode).unblock(WALRecord.RecordType.TX_RECORD);
    }
}
