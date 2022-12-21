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
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/** */
public class ConsistentCutNodeFailureTest extends AbstractConsistentCutTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** */
    @Test
    public void shouldSkipFinishRecordAfterTransactionRecovery() throws Exception {
        runConsistentCutAndBreak(() -> {
            stopGrid(nodes());

            return "Cut is inconsistent";
        });
    }

    /** */
    @Test
    public void shouldSkipFinishRecordAfterNodeFailure() throws Exception {
        runConsistentCutAndBreak(() -> {
            stopGrid(1);

            return "Snapshot operation interrupted, because baseline node left the cluster";
        });
    }

    /** */
    private void runConsistentCutAndBreak(Supplier<String> breakCutWithExcp) throws Exception {
        Ignite cln = grid(nodes());

        TestRecordingCommunicationSpi clnComm = TestRecordingCommunicationSpi.spi(cln);

        clnComm.blockMessages((n, msg) -> msg.getClass() == GridNearTxFinishRequest.class);

        IgniteInternalFuture<?> loadFut = asyncRunTx();

        clnComm.waitForBlocked();

        awaitSnapshotResourcesCleaned();

        IgniteFuture<Void> snpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

        waitForCutIsStartedOnAllNodes();

        UUID brokenCutId = snp(grid(0)).consistentCutId();

        String excMsg = breakCutWithExcp.get();

        if (G.allGrids().contains(cln)) {
            clnComm.stopBlock();

            loadFut.get();
        }

        GridTestUtils.assertThrows(log, () -> snpFut.get(), IgniteException.class, excMsg);

        awaitSnapshotResourcesCleaned();

        for (Ignite g: G.allGrids())
            assertNull(snp((IgniteEx)g).consistentCutId());

        stopAllGrids();

        for (int i = 0; i < nodes(); i++)
            assertWalConsistentRecords(i, brokenCutId);
    }

    /** */
    private void assertWalConsistentRecords(int nodeIdx, UUID brokenCutId) throws Exception {
        WALIterator iter = walIter(nodeIdx);

        boolean reachInconsistent = false;

        while (iter.hasNext()) {
            WALRecord rec = iter.next().getValue();

            if (rec.type() == WALRecord.RecordType.CONSISTENT_CUT_START_RECORD) {
                ConsistentCutStartRecord startRec = (ConsistentCutStartRecord)rec;

                assertEquals(brokenCutId, startRec.cutId());

                reachInconsistent = true;
            }
            else
                assert rec.type() != WALRecord.RecordType.CONSISTENT_CUT_FINISH_RECORD : "Unexpect Finish Record.";
        }

        assertTrue("Should reach StartRecord for bad snapshot", reachInconsistent);
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
                allNodeStartedCut &= snp(grid(i)).consistentCutId() != null;

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
