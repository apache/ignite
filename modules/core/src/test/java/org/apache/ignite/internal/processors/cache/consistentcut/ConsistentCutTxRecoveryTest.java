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

import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
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

/***/
public class ConsistentCutTxRecoveryTest extends AbstractConsistentCutBlockingTest {
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

            ConsistentCutMarker blkCutMarker = BlockingConsistentCutManager.cutMgr(grid(0)).runningCutMarker();

            // Stop client node.
            stopGrid(nodes());

            loadFut.cancel();

            GridTestUtils.assertThrows(log, () -> snpFut.get(), IgniteException.class, "Cut is inconsistent");

            awaitAllNodesReadyForIncrementalSnapshot();

            snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout());

            // Stop cluster with flushing WALs.
            stopCluster();

            for (int i = 0; i < nodes(); i++)
                assertWalConsistentRecords(i, blkCutMarker);
        }
        finally {
            if (loadFut != null)
                loadFut.cancel();
        }
    }

    /** */
    private void assertWalConsistentRecords(int nodeIdx, ConsistentCutMarker blkMarker) throws Exception {
        WALIterator iter = walIter(nodeIdx);

        boolean expFinRec = false;
        boolean reachInconsistent = false;

        long lastVerChecked = 0;

        while (iter.hasNext()) {
            WALRecord rec = iter.next().getValue();

            if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_START_RECORD) {
                ConsistentCutStartRecord startRec = (ConsistentCutStartRecord)rec;

                expFinRec = !startRec.marker().id().equals(blkMarker.id());

                if (!expFinRec)
                    reachInconsistent = true;

                lastVerChecked = startRec.marker().index();
            }
            else if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD) {
                assertTrue("Unexpect Finish Record. " + blkMarker, expFinRec);

                expFinRec = false;
            }
        }

        assertTrue("Should reach StartRecord for bad snapshot", reachInconsistent);
        assertTrue("Should reach marker after " + blkMarker, lastVerChecked == blkMarker.index());
    }

    /** */
    private IgniteInternalFuture<?> asyncRunTx() throws Exception {
        return multithreadedAsync(() -> {
            Random r = new Random();

            // Start on the client node.
            Ignite g = grid(nodes());

            try (Transaction tx = g.transactions().txStart()) {
                for (int j = 0; j < nodes(); j++) {
                    IgniteCache<Integer, Integer> cache = g.cache(CACHE);

                    int k = key(CACHE, grid(j).localNode(), grid((j + 1) % nodes()).localNode());

                    cache.put(k, r.nextInt());
                }

                tx.commit();
            }
        }, 1);
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
