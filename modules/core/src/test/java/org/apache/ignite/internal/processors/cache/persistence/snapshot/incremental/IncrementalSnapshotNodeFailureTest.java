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
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/** */
public class IncrementalSnapshotNodeFailureTest extends AbstractIncrementalSnapshotTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** */
    @Test
    public void shouldSkipFinishRecordAfterTransactionRecovery() throws Exception {
        runIncrementalSnapshotAndBreak(() -> {
            stopGrid(nodes());

            return "Incremental snapshot is inconsistent";
        });
    }

    /** */
    @Test
    public void shouldSkipFinishRecordAfterNodeFailure() throws Exception {
        runIncrementalSnapshotAndBreak(() -> {
            stopGrid(1);

            return "Snapshot operation interrupted, because baseline node left the cluster";
        });
    }

    /** */
    private void runIncrementalSnapshotAndBreak(Supplier<String> breakSnpWithExcp) throws Exception {
        Ignite cln = grid(nodes());

        TestRecordingCommunicationSpi clnComm = TestRecordingCommunicationSpi.spi(cln);

        clnComm.blockMessages((n, msg) -> msg.getClass() == GridNearTxFinishRequest.class);

        IgniteInternalFuture<?> loadFut = asyncRunTx();

        clnComm.waitForBlocked();

        awaitSnapshotResourcesCleaned();

        IgniteFuture<Void> snpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (int i = 0; i < nodes(); i++) {
                if (snp(grid(i)).incrementalSnapshotId() == null)
                    return false;
            }

            return true;
        }, getTestTimeout(), 10));

        UUID brokenSnpId = snp(grid(0)).incrementalSnapshotId();

        String excMsg = breakSnpWithExcp.get();

        if (G.allGrids().contains(cln)) {
            clnComm.stopBlock();

            loadFut.get();
        }

        GridTestUtils.assertThrows(log, () -> snpFut.get(), IgniteException.class, excMsg);

        awaitSnapshotResourcesCleaned();

        for (Ignite g: G.allGrids())
            assertNull(snp((IgniteEx)g).incrementalSnapshotId());

        stopAllGrids();

        for (int i = 0; i < nodes(); i++)
            assertWalSnapshotRecords(i, brokenSnpId);
    }

    /** */
    private void assertWalSnapshotRecords(int nodeIdx, UUID brokenSnpId) throws Exception {
        try (WALIterator iter = walIter(nodeIdx)) {
            boolean reachInconsistent = false;

            while (iter.hasNext()) {
                WALRecord rec = iter.next().getValue();

                if (rec.type() == WALRecord.RecordType.INCREMENTAL_SNAPSHOT_START_RECORD) {
                    IncrementalSnapshotStartRecord startRec = (IncrementalSnapshotStartRecord)rec;

                    assertEquals(brokenSnpId, startRec.id());

                    reachInconsistent = true;
                }
                else
                    assert rec.type() != WALRecord.RecordType.INCREMENTAL_SNAPSHOT_FINISH_RECORD : "Unexpect Finish Record.";
            }

            assertTrue("Should reach StartRecord for bad snapshot", reachInconsistent);
        }
    }

    /** */
    private IgniteInternalFuture<?> asyncRunTx() throws Exception {
        return multithreadedAsync(() -> {
            // Start on the client node.
            Ignite g = grid(nodes());

            try (Transaction tx = g.transactions().txStart()) {
                for (int i = 0; i < nodes(); i++) {
                    IgniteCache<Integer, Integer> cache = g.cache(CACHE);

                    cache.put(TransactionTestCase.key((IgniteEx)g, CACHE, i, (i + 1) % nodes()), 0);
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
