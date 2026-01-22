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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryIoSession;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/** */
public class IncrementalSnapshotJoiningClientTest extends AbstractIncrementalSnapshotTest {
    /** */
    private static volatile CountDownLatch blockClientJoinReq;

    /** */
    private static volatile CountDownLatch unblockClientJoinReq;

    /** */
    private static volatile CountDownLatch acceptClientReq;

    /** */
    private static volatile CountDownLatch addClient;

    /** */
    private static volatile CountDownLatch rcvStartSnpReq;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setSnapshotThreadPoolSize(1);

        if (getTestIgniteInstanceIndex(instanceName) == 0) {
            cfg.setDiscoverySpi(new CoordinatorBlockingDiscoverySpi()
                .setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder()));
        }

        if (getTestIgniteInstanceIndex(instanceName) == nodes() + 1) {
            cfg.setDiscoverySpi(new ClientBlockingDiscoverySpi()
                .setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder()));
        }

        return cfg;
    }

    /** */
    @Test
    public void testJoiningClientShouldInitLocalSnapshot() throws Exception {
        checkClientAwarenessOfSnapshot(true, () -> {
            unblockClientJoinReq.countDown();

            // Await for the client occupied the discovery thread.
            U.awaitQuiet(acceptClientReq);

            // Snapshot awaits for the discovery thread unblocked.
            IgniteFuture<Void> snpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

            addClient.countDown();

            return snpFut;
        });
    }

    /** */
    @Test
    public void testJoiningClientShouldNotInitLocalSnapshot() throws Exception {
        checkClientAwarenessOfSnapshot(false, () -> {
            IgniteFuture<Void> snpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

            // Await for the snapshot occupied the discovery thread.
            U.awaitQuiet(rcvStartSnpReq);

            // Client will be blocked waiting for discovery thread.
            unblockClientJoinReq.countDown();
            addClient.countDown();

            return snpFut;
        });
    }

    /**
     * Check if client should know about incremental snapshot or not, depending on order of client start and snapshot request.
     */
    private void checkClientAwarenessOfSnapshot(boolean clnShouldStartSnapshot, Supplier<IgniteFuture<Void>> start) throws Exception {
        rcvStartSnpReq = new CountDownLatch(1);
        acceptClientReq = new CountDownLatch(1);
        unblockClientJoinReq = new CountDownLatch(1);
        blockClientJoinReq = new CountDownLatch(1);
        addClient = new CountDownLatch(1);

        IgniteInternalFuture<?> newClnFut = multithreadedAsync(() -> startClientGrid(nodes() + 1), 1);

        // Await client is about to start. Wait only TcpDiscoveryNodeAddedMessage.
        U.awaitQuiet(blockClientJoinReq);

        CountDownLatch locSnpStart = new CountDownLatch(1);

        grid(0).context().pools().getSnapshotExecutorService().submit(() -> U.awaitQuiet(locSnpStart));

        IgniteFuture<Void> snpFut = start.get();

        newClnFut.get(getTestTimeout());

        IgniteUuid txId = runTx(grid(nodes() + 1), 0, 100);

        if (clnShouldStartSnapshot)
            assertNotNull(snp(grid(nodes() + 1)).incrementalSnapshotId());
        else
            assertNull(snp(grid(nodes() + 1)).incrementalSnapshotId());

        locSnpStart.countDown();

        snpFut.get(getTestTimeout());

        assertTrue(transactionExcluded(0, txId));

        checkRestoredSnapshotIsEmpty();
    }

    /** Limit bounds for different threads to avoid locks. */
    private IgniteUuid runTx(IgniteEx g, int from, int to) {
        try (Transaction tx = g.transactions().txStart()) {
            for (int j = 0; j < 10; j++) {
                IgniteCache<Integer, Integer> cache = g.cache(CACHE);

                cache.put(from + ThreadLocalRandom.current().nextInt(to - from), 0);
            }

            tx.commit();

            return tx.xid();
        }
    }

    /** */
    private boolean transactionExcluded(int nodeIdx, IgniteUuid txId) throws Exception {
        try (WALIterator iter = walIter(nodeIdx)) {
            while (iter.hasNext()) {
                WALRecord rec = iter.next().getValue();

                if (rec.type() == WALRecord.RecordType.INCREMENTAL_SNAPSHOT_FINISH_RECORD) {
                    IncrementalSnapshotFinishRecord finRec = (IncrementalSnapshotFinishRecord)rec;

                    assertTrue(finRec.excluded().stream().anyMatch(id -> id.asIgniteUuid().equals(txId)));

                    return true;
                }
            }
        }

        return false;
    }

    /** */
    private void checkRestoredSnapshotIsEmpty() throws Exception {
        stopAllGrids();

        cleanPersistenceDir(true);

        Ignite g = startGrids(nodes());

        g.cluster().state(ClusterState.ACTIVE);

        g.destroyCache(CACHE);

        awaitPartitionMapExchange();

        g.snapshot().restoreSnapshot(SNP, null, 1).get(getTestTimeout());

        assertPartitionsSame(idleVerify(grid(0)));

        assertEquals(0, grid(0).cache(CACHE).size());
    }

    /** */
    private static class ClientBlockingDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void writeMessage(
            TcpDiscoveryIoSession ses,
            TcpDiscoveryAbstractMessage msg,
            long timeout
        ) throws IOException, IgniteCheckedException {
            if (msg instanceof TcpDiscoveryJoinRequestMessage && blockClientJoinReq != null) {
                blockClientJoinReq.countDown();

                U.awaitQuiet(unblockClientJoinReq);
            }

            super.writeMessage(ses, msg, timeout);
        }
    }

    /** */
    private static class CoordinatorBlockingDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryNodeAddedMessage && acceptClientReq != null) {
                acceptClientReq.countDown();

                U.awaitQuiet(addClient);
            }

            if (msg instanceof TcpDiscoveryCustomEventMessage && rcvStartSnpReq != null) {
                TcpDiscoveryCustomEventMessage m = (TcpDiscoveryCustomEventMessage)msg;

                try {
                    m.finishUnmarhal(marshaller(), U.resolveClassLoader(ignite().configuration()));

                    DiscoveryCustomMessage m0 = m.message();

                    DiscoveryCustomMessage realMsg = GridTestUtils.unwrap(m0);

                    if (realMsg instanceof InitMessage)
                        rcvStartSnpReq.countDown();
                }
                catch (Throwable e) {
                    // No-op.
                }
            }
        }
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
