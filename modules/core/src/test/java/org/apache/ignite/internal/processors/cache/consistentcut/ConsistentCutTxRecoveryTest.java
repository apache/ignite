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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/** */
public class ConsistentCutTxRecoveryTest extends AbstractConsistentCutTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** */
    @Test
    public void testSkipFinishRecordOnTxRecovery() throws Exception {
        IgniteInternalFuture<?> loadFut = null;

        try {
            snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout());

            // Blocks finish message from client to server.
            TestRecordingCommunicationSpi clnComm = TestRecordingCommunicationSpi.spi(grid(nodes()));

            clnComm.blockMessages(GridNearTxFinishRequest.class, grid(0).name());

            loadFut = asyncRunTx();

            clnComm.waitForBlocked();

            awaitAllNodesReadyForIncrementalSnapshot();

            IgniteFuture<Void> snpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

            waitForCutIsStartedOnAllNodes();

            UUID blkCutId = TestConsistentCutManager.cutMgr(grid(0)).consistentCut().id();

            // Stop client node.
            stopGrid(nodes());

            loadFut.cancel();

            GridTestUtils.assertThrows(log, () -> snpFut.get(), IgniteException.class, "Cut is inconsistent");

            awaitAllNodesReadyForIncrementalSnapshot();

            snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout());

            stopAllGrids();

            for (int i = 0; i < nodes(); i++)
                assertWalConsistentRecords(i, blkCutId, 2);
        }
        finally {
            if (loadFut != null)
                loadFut.cancel();
        }
    }

    /** */
    private void assertWalConsistentRecords(int nodeIdx, UUID blkCutId, int incSnpCnt) throws Exception {
        WALIterator iter = walIter(nodeIdx);

        boolean expFinRec = false;
        boolean reachInconsistent = false;

        int actIncSnpCnt = 0;

        while (iter.hasNext()) {
            WALRecord rec = iter.next().getValue();

            if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_START_RECORD) {
                ConsistentCutStartRecord startRec = (ConsistentCutStartRecord)rec;

                expFinRec = !startRec.cutId().equals(blkCutId);

                if (!expFinRec)
                    reachInconsistent = true;
            }
            else if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD) {
                assertTrue("Unexpect Finish Record. " + blkCutId, expFinRec);

                expFinRec = false;

                actIncSnpCnt++;
            }
        }

        assertTrue("Should reach StartRecord for bad snapshot", reachInconsistent);
        assertEquals("Should reach blkCutId after " + blkCutId, incSnpCnt, actIncSnpCnt);
    }

    /** */
    private IgniteInternalFuture<?> asyncRunTx() throws Exception {
        return multithreadedAsync(() -> {
            // Start on the client node.
            Ignite g = grid(nodes());

            try (Transaction tx = g.transactions().txStart()) {
                for (int j = 0; j < 10; j++) {
                    IgniteCache<Integer, Integer> cache = g.cache(CACHE);

                    cache.put(j, j);
                }

                tx.commit();
            }
        }, 1);
    }

    /** */
    private void waitForCutIsStartedOnAllNodes() throws Exception {
        GridTestUtils.waitForCondition(() -> {
            boolean allNodeStartedCut = true;

            for (int i = 0; i < nodes(); i++)
                allNodeStartedCut &= TestConsistentCutManager.cutMgr(grid(i)).consistentCut() != null;

            return allNodeStartedCut;
        }, getTestTimeout(), 10);
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 2;
    }
}
