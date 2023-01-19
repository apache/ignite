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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.transactions.TransactionConcurrency;

/** */
public abstract class AbstractIncrementalSnapshotMessagesBlockingTest extends AbstractIncrementalSnapshotBlockingTest {
    /** */
    private static Class<?> txMsgBlkCls;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** Initialize latches for test cases with blocking tx messages. */
    protected final void initMsgCase(Class<?> msgCls, BlkNodeType txBlkNode, BlkSnpType snpBlkType, BlkNodeType snpBlkNode) {
        txBlkNodeType = txBlkNode;

        txMsgBlkCls = msgCls;
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

        int snpBlkNodeIdx = -1;

        if (snpBlkType != BlkSnpType.NONE)
            snpBlkNodeIdx = blkNodeIndex(nearNodeIdx, snpBlkNodeType, testCase);

        if (skipMsgTestCase(txBlkNodeIdx, nearNodeIdx, testCase))
            return;

        log.info("START CASE " + caseNum +
            ". Data=" + testCase +
            ", nearNodeIdx=" + nearNodeIdx +
            ", txBlkNodeIdx=" + txBlkNodeIdx +
            ", txBlkNodeType=" + txBlkNodeType +
            ", snpBlkNodeIdx=" + snpBlkNodeIdx +
            ", msg=" + txMsgBlkCls.getSimpleName());

        run(() -> tx(nearNodeIdx, testCase, txConcurrency), txBlkNodeIdx, snpBlkNodeIdx);
    }

    /** {@inheritDoc} */
    @Override protected void blockTx(IgniteEx blkNode) {
        TestRecordingCommunicationSpi.spi(blkNode).blockMessages((n, msg) ->
            msg.getClass().equals(txMsgBlkCls)
        );
    }

    /** {@inheritDoc} */
    @Override protected void unblockTx(IgniteEx blkNode) {
        TestRecordingCommunicationSpi.spi(blkNode).stopBlock();
    }

    /** {@inheritDoc} */
    @Override protected void awaitTxBlocked(IgniteEx blkNode) throws Exception {
        TestRecordingCommunicationSpi.spi(blkNode).waitForBlocked(1);
    }

    /**
     * Test cases are auto-generated and some of them are invalid, when node {@code #txBlkNodeIdx} that should block
     * a transaction on sending {@link #txMsgBlkCls} actually doesn't participate in sending such message. Basing on
     * transctional protocol it checks whether specified test case is invalid and return {@code true} if it is.
     *
     * @return true if test case should be skipped.
     */
    private boolean skipMsgTestCase(int txBlkNodeIdx, int nearNodeIdx, TransactionTestCase testCase) {
        // GridNearTxPrepareRequest is sent only from near node if at least one primary copy isn't collocated.
        if (txMsgBlkCls.equals(GridNearTxPrepareRequest.class)) {
            return txBlkNodeType != BlkNodeType.NEAR || testCase.allPrimaryOnNear(nearNodeIdx);
        }

        // GridNearTxPrepareResponse is sent only from primary node if it isn't collocated on near node.
        if (txMsgBlkCls.equals(GridNearTxPrepareResponse.class)) {
            return txBlkNodeType != BlkNodeType.PRIMARY || txBlkNodeIdx == nearNodeIdx;
        }

        // GridNearTxFinishRequest is sent only from near node for two-phase-commit and if at least one primary isn't collocated.
        if (txMsgBlkCls.equals(GridNearTxFinishRequest.class)) {
            return txBlkNodeType != BlkNodeType.NEAR || testCase.onePhase() || testCase.allPrimaryOnNear(nearNodeIdx);
        }

        // GridNearTxFinishResponse is sent only from primary node for two-phase-commit and if it isn't collocated.
        if (txMsgBlkCls.equals(GridNearTxFinishResponse.class)) {
            return txBlkNodeType != BlkNodeType.PRIMARY || testCase.onePhase() || txBlkNodeIdx == nearNodeIdx;
        }

        // GridDhtTxPrepareRequest is sent from primary node, or from near node if primaries are collocated.
        if (txMsgBlkCls.equals(GridDhtTxPrepareRequest.class)) {
            if (txBlkNodeType == BlkNodeType.NEAR)
                return !testCase.allPrimaryOnNear(nearNodeIdx);

            return txBlkNodeType != BlkNodeType.PRIMARY;
        }

        // GridDhtTxPrepareResponse is sent from backup nodes only.
        if (txMsgBlkCls.equals(GridDhtTxPrepareResponse.class))
            return txBlkNodeType != BlkNodeType.BACKUP;

        // GridDhtTxFinishRequest is sent from primary node for two-phase-commit, or from near node if primaries are collocated.
        if (txMsgBlkCls.equals(GridDhtTxFinishRequest.class)) {
            if (testCase.onePhase())
                return true;

            if (txBlkNodeType == BlkNodeType.NEAR)
                return !testCase.allPrimaryOnNear(nearNodeIdx);

            return txBlkNodeType != BlkNodeType.PRIMARY;
        }

        return false;
    }

    /** */
    protected static List<Class<?>> messages(boolean backup) {
        List<Class<?>> msgCls = new ArrayList<>();

        msgCls.add(GridNearTxPrepareRequest.class);
        msgCls.add(GridNearTxPrepareResponse.class);
        msgCls.add(GridNearTxFinishRequest.class);
        msgCls.add(GridNearTxFinishResponse.class);

        if (backup) {
            msgCls.add(GridDhtTxPrepareRequest.class);
            msgCls.add(GridDhtTxPrepareResponse.class);
            msgCls.add(GridDhtTxFinishRequest.class);
        }

        return msgCls;
    }
}
