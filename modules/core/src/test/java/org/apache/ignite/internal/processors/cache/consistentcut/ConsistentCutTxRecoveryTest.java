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
import java.util.UUID;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.consistentcut.TransactionTestCase.key;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/***/
public class ConsistentCutTxRecoveryTest extends AbstractConsistentCutTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** In case of rollback scenario with failing client incremental snapshot should succeed. */
    @Test
    public void testNotSkipFinishRecordTxRecoveryRollbacked() throws Exception {
        forceTransactionRecoveryAndCheckConsistentCut((n, msg) ->
            n.equals(grid(nodes() - 1).localNode()) && msg instanceof GridNearTxPrepareRequest, false);
    }

    /** In case of commit scenario with failing client incremental snapshot should fail. */
    @Test
    public void testSkipFinishRecordOnTxRecoveryCommitted() throws Exception {
        forceTransactionRecoveryAndCheckConsistentCut((n, msg) -> msg instanceof GridNearTxFinishRequest, true);
    }

    /** */
    private void forceTransactionRecoveryAndCheckConsistentCut(
        IgniteBiPredicate<ClusterNode, Message> p,
        boolean shouldFail
    ) throws Exception {
        IgniteInternalFuture<?> loadFut = null;

        try {
            snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout());

            TestRecordingCommunicationSpi srvComm = TestRecordingCommunicationSpi.spi(grid(0));
            TestRecordingCommunicationSpi clnComm = TestRecordingCommunicationSpi.spi(grid(nodes()));

            srvComm.record(GridNearTxPrepareResponse.class);
            clnComm.blockMessages(p);

            loadFut = asyncRunTx();

            clnComm.waitForBlocked();
            srvComm.waitForRecorded();

            awaitSnapshotResourcesCleaned();

            IgniteFuture<Void> snpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

            // Wait for consistent cut started.
            GridTestUtils.waitForCondition(() -> snp(grid(0)).consistentCutId() != null, getTestTimeout(), 10);

            UUID failCutId = shouldFail ? snp(grid(0)).consistentCutId() : null;

            // Stop client node.
            stopGrid(nodes());

            loadFut.cancel();

            if (shouldFail)
                GridTestUtils.assertThrows(log, () -> snpFut.get(), IgniteException.class, "Cut is inconsistent");
            else
                snpFut.get();

            awaitSnapshotResourcesCleaned();

            snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout());

            for (int i = 0; i < nodes(); i++)
                assertWalConsistentRecords(i, failCutId);
        }
        finally {
            if (loadFut != null)
                loadFut.cancel();
        }
    }

    /** */
    private void assertWalConsistentRecords(int nodeIdx, @Nullable UUID failCutId) throws Exception {
        try (WALIterator iter = walIter(nodeIdx)) {
            boolean expFinRec = false;

            int actIncSnpCnt = 0;

            while (iter.hasNext()) {
                WALRecord rec = iter.next().getValue();

                if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_START_RECORD) {
                    ConsistentCutStartRecord startRec = (ConsistentCutStartRecord)rec;

                    expFinRec = !startRec.id().equals(failCutId);
                }
                else if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD) {
                    assertTrue("Unexpect Finish Record: " + failCutId, expFinRec);

                    expFinRec = false;

                    actIncSnpCnt++;
                }
            }

            assertEquals("Incorrect count of FinishRecords: " + actIncSnpCnt, failCutId == null ? 3 : 2, actIncSnpCnt);
        }
    }

    /** */
    private IgniteInternalFuture<?> asyncRunTx() throws Exception {
        return multithreadedAsync(() -> {
            Random r = new Random();

            // Start on the client node.
            IgniteEx g = grid(nodes());

            try (Transaction tx = g.transactions().txStart()) {
                for (int j = 0; j < nodes(); j++) {
                    IgniteCache<Integer, Integer> cache = g.cache(CACHE);

                    int k = key(g, CACHE, j, (j + 1) % nodes());

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
