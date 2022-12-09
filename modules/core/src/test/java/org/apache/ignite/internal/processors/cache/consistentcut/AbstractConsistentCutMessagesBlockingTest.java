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

import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.BACKUP;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.NEAR;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.PRIMARY;

/** */
public abstract class AbstractConsistentCutMessagesBlockingTest extends AbstractConsistentCutBlockingTest {
    /** */
    private static Class<?> txMsgBlkCls;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** Initialize latches for test cases with blocking tx messages. */
    protected final void initMsgCase(Class<?> msgCls, BlkNodeType txBlkNode, BlkCutType cutBlkType, BlkNodeType cutBlkNode) {
        txBlkNodeType = txBlkNode;

        txMsgBlkCls = msgCls;
        AbstractConsistentCutBlockingTest.cutBlkType = cutBlkType;
        cutBlkNodeType = cutBlkNode;
    }

    /** */
    @Override protected void runCase(TransactionTestCase testCase, int nearNode, TransactionConcurrency txConcurrency) throws Exception {
        int txBlkNodeId = blkNode(nearNode, txBlkNodeType, testCase);

        int cutBlkNodeId = -1;

        if (cutBlkType != BlkCutType.NONE)
            cutBlkNodeId = blkNode(nearNode, cutBlkNodeType, testCase);

        if (skipMsgTestCase(txBlkNodeId, nearNode, testCase))
            return;

        log.info("START CASE " + caseNum +
            ". Data=" + testCase +
            ", nearNode=" + nearNode +
            ", txBlkNodeId=" + txBlkNodeId +
            ", txBlkNodeType=" + txBlkNodeType +
            ", cutBlkNodeId=" + cutBlkNodeId +
            ", msg=" + txMsgBlkCls.getSimpleName());

        run(() -> tx(nearNode, testCase, txConcurrency), txBlkNodeId, cutBlkNodeId);
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

    /** */
    private boolean skipMsgTestCase(int txBlkNodeId, int nearNodeId, TransactionTestCase testCase) {
        if (txMsgBlkCls.equals(GridNearTxPrepareRequest.class)) {
            if (txBlkNodeType != NEAR)
                return true;

            return testCase.allPrimaryOnNear(nearNodeId);
        }

        if (txMsgBlkCls.equals(GridNearTxPrepareResponse.class)) {
            if (txBlkNodeType != PRIMARY || txBlkNodeId == nearNodeId)
                return true;

            return testCase.allPrimaryOnNear(nearNodeId);
        }

        if (txMsgBlkCls.equals(GridNearTxFinishRequest.class)) {
            if (txBlkNodeType != NEAR || testCase.onePhase())
                return true;

            return testCase.allPrimaryOnNear(nearNodeId);
        }

        if (txMsgBlkCls.equals(GridNearTxFinishResponse.class)) {
            if (txBlkNodeType != PRIMARY || testCase.onePhase() || txBlkNodeId == nearNodeId)
                return true;

            return testCase.allPrimaryOnNear(nearNodeId);
        }

        if (txMsgBlkCls.equals(GridDhtTxPrepareRequest.class)) {
            // Near node might send the request to the backup nodes in case if it is collocated.
            if (txBlkNodeType == NEAR)
                return !testCase.allPrimaryOnNear(nearNodeId);

            return txBlkNodeType == BACKUP;
        }

        if (txMsgBlkCls.equals(GridDhtTxPrepareResponse.class))
            return txBlkNodeType != BACKUP;

        if (txMsgBlkCls.equals(GridDhtTxFinishRequest.class)) {
            if (txBlkNodeType == BACKUP || testCase.onePhase())
                return true;

            // Near node might send the request to the backup nodes in case if near node is collocated.
            if (txBlkNodeType == NEAR)
                return !testCase.allPrimaryOnNear(nearNodeId);

            return false;
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
