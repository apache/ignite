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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/** */
public class IncrementalSnapshotJoiningClientTest extends AbstractIncrementalSnapshotTest {
    /** */
    private static final Random RND = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setSnapshotThreadPoolSize(1);

        return cfg;
    }

    /** */
    @Test
    public void testJoiningNodeShouldNotInitLocalSnapshot() throws Exception {
        TestRecordingCommunicationSpi srvComm = TestRecordingCommunicationSpi.spi(grid(0));

        AtomicBoolean blk = new AtomicBoolean();

        srvComm.blockMessages((n, msg) ->
            msg instanceof GridNearTxPrepareResponse && blk.compareAndSet(false, true));

        IgniteInternalFuture<?> tx = multithreadedAsync(() -> runTx(grid(nodes()), 0, 100), 1);

        srvComm.waitForBlocked();

        IgniteFuture<Void> snpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

        // Wait for incremental snapshot started on all server nodes.
        assertTrue(GridTestUtils
            .waitForCondition(() -> snp(grid(0)).incrementalSnapshotId() != null, getTestTimeout(), 10));

        IgniteEx newCln = startClientGrid(nodes() + 1);

        runTx(newCln, 100, 200);

        assertNull(snp(newCln).incrementalSnapshotId());

        srvComm.stopBlock();

        tx.get(getTestTimeout());

        snpFut.get(getTestTimeout());

        checkRestoredCache(1);
    }

    /** */
    @Test
    public void testTransactionFromJoiningNodeIsExcluded() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        // Block starting of incremental snapshot on one node.
        grid(1).context().pools().getSnapshotExecutorService().submit(() -> U.awaitQuiet(latch));

        IgniteFuture<Void> snpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

        IgniteEx newCln = startClientGrid(nodes() + 1);

        IgniteUuid txId = runTx(newCln, 0, 100);

        latch.countDown();

        snpFut.get(getTestTimeout());

        assertTransactionExcluded(1, txId);

        checkRestoredCache(1);
    }

    /** */
    private void checkRestoredCache(int incIdx) throws Exception {
        stopAllGrids();

        cleanPersistenceDir(true);

        Ignite g = startGrids(nodes());

        g.cluster().state(ClusterState.ACTIVE);

        g.destroyCache(CACHE);

        awaitPartitionMapExchange();

        g.snapshot().restoreIncrementalSnapshot(SNP, null, incIdx).get(getTestTimeout());

        assertPartitionsSame(idleVerify(grid(0)));

        assertEquals(0, grid(0).cache(CACHE).size());
    }

    /** Limit bounds for different threads to avoid locks. */
    private IgniteUuid runTx(IgniteEx g, int from, int to) {
        try (Transaction tx = g.transactions().txStart()) {
            for (int j = 0; j < 10; j++) {
                IgniteCache<Integer, Integer> cache = g.cache(CACHE);

                cache.put(from + RND.nextInt(to - from), 0);
            }

            tx.commit();

            return tx.xid();
        }
    }

    /** */
    private void assertTransactionExcluded(int nodeIdx, IgniteUuid txId) throws Exception {
        boolean checked = false;

        try (WALIterator iter = walIter(nodeIdx)) {
            while (iter.hasNext()) {
                WALRecord rec = iter.next().getValue();

                if (rec.type() == WALRecord.RecordType.INCREMENTAL_SNAPSHOT_FINISH_RECORD) {
                    IncrementalSnapshotFinishRecord finRec = (IncrementalSnapshotFinishRecord)rec;

                    assertTrue(finRec.excluded().stream().anyMatch(id -> id.asIgniteUuid().equals(txId)));

                    checked = true;
                }
            }
        }

        assertTrue(checked);
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
