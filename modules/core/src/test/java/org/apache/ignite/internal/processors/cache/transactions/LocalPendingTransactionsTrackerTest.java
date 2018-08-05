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
package org.apache.ignite.internal.processors.cache.transactions;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridDebug;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link LocalPendingTransactionsTracker}
 */
public class LocalPendingTransactionsTrackerTest {
    /** Timeout executor. */
    private static ScheduledExecutorService timeoutExecutor;

    /** Tracker. */
    private LocalPendingTransactionsTracker tracker;

    /**
     *
     */
    @BeforeClass
    public static void setUpClass() {
        timeoutExecutor = new ScheduledThreadPoolExecutor(1);

        U.onGridStart();

        System.setProperty(IgniteSystemProperties.IGNITE_PENDING_TX_TRACKER_ENABLED, "true");
    }

    /**
     *
     */
    @AfterClass
    public static void tearDownClass() {
        timeoutExecutor.shutdown();

        System.clearProperty(IgniteSystemProperties.IGNITE_PENDING_TX_TRACKER_ENABLED);
    }

    /**
     *
     */
    @Before
    public void setUp() {
        GridTimeoutProcessor time = Mockito.mock(GridTimeoutProcessor.class);
        Mockito.when(time.addTimeoutObject(Mockito.any())).thenAnswer(mock -> {
            GridTimeoutObject timeoutObj = (GridTimeoutObject)mock.getArguments()[0];

            long endTime = timeoutObj.endTime();

            timeoutExecutor.schedule(timeoutObj::onTimeout, endTime - U.currentTimeMillis(), TimeUnit.MILLISECONDS);

            return null;
        });

        GridCacheSharedContext<?, ?> cctx = Mockito.mock(GridCacheSharedContext.class);
        Mockito.when(cctx.time()).thenReturn(time);
        Mockito.when(cctx.logger(LocalPendingTransactionsTracker.class)).thenReturn(new GridTestLog4jLogger());

        tracker = new LocalPendingTransactionsTracker(cctx);
    }

    /**
     *
     */
    @Test
    public void testCurrentlyPreparedTxs() {
        txPrepare(1);
        txKeyWrite(1, 10);
        txKeyWrite(1, 11);

        txPrepare(2);
        txKeyWrite(2, 20);
        txKeyWrite(2, 21);
        txKeyWrite(2, 22);

        txPrepare(3);
        txKeyWrite(3, 30);

        txCommit(2);

        tracker.writeLockState();

        try {
            Map<GridCacheVersion, WALPointer> currentlyPreparedTxs = tracker.currentlyPreparedTxs();

            assertEquals(2, currentlyPreparedTxs.size());
            assertTrue(currentlyPreparedTxs.containsKey(nearXidVersion(1)));
            assertTrue(currentlyPreparedTxs.containsKey(nearXidVersion(3)));
        }
        finally {
            tracker.writeUnlockState();
        }

        txKeyWrite(3, 31);
        txCommit(3);

        tracker.writeLockState();

        try {
            Map<GridCacheVersion, WALPointer> currentlyPreparedTxs = tracker.currentlyPreparedTxs();

            assertEquals(1, currentlyPreparedTxs.size());
            assertTrue(currentlyPreparedTxs.containsKey(nearXidVersion(1)));
        }
        finally {
            tracker.writeUnlockState();
        }
    }

    /**
     *
     */
    @Test
    public void testMultiplePrepareCommitMarkers() {
        txPrepare(1);
        txKeyWrite(1, 10);

        txPrepare(2);
        txKeyWrite(2, 20);
        txPrepare(2);
        txKeyWrite(2, 21);
        txPrepare(2);
        txKeyWrite(2, 22);

        txPrepare(3);
        txKeyWrite(3, 30);
        txPrepare(3);
        txKeyWrite(3, 31);

        txCommit(3);
        txCommit(3);

        txCommit(1);

        txCommit(2);
        txCommit(2);

        tracker.writeLockState();

        try {
            Map<GridCacheVersion, WALPointer> currentlyPreparedTxs = tracker.currentlyPreparedTxs();

            assertEquals(1, currentlyPreparedTxs.size());
            assertTrue(currentlyPreparedTxs.containsKey(nearXidVersion(2)));
        }
        finally {
            tracker.writeUnlockState();
        }
    }

    /**
     *
     */
    @Test
    public void testCommitsMoreThanPreparesForbidden() {
        txPrepare(1);

        txKeyWrite(1, 10);
        txKeyWrite(1, 11);

        txCommit(1);

        try {
            txCommit(1);

            fail("We should fail if number of commits is more than number of prepares.");
        }
        catch (Throwable ignored) {
            // Expected.
        }
    }

    /**
     *
     */
    @Test
    public void testRollback() {
        txRollback(1); // Tx can be rolled back before prepare.

        txPrepare(2);
        txKeyWrite(2, 20);

        txPrepare(3);
        txKeyWrite(3, 30);
        txPrepare(3);
        txKeyWrite(3, 31);

        txCommit(3);

        txRollback(2);
        txRollback(3);

        tracker.writeLockState();

        try {
            Map<GridCacheVersion, WALPointer> currentlyPreparedTxs = tracker.currentlyPreparedTxs();

            assertEquals(0, currentlyPreparedTxs.size());
        }
        finally {
            tracker.writeUnlockState();
        }
    }

    /**
     *
     */
    @Test(timeout = 10_000)
    public void testAwaitFinishOfPreparedTxs() throws Exception {
        txPrepare(1);

        txPrepare(2);
        txPrepare(2);

        txPrepare(3);
        txPrepare(3);
        txCommit(3);

        txPrepare(4);
        txCommit(4);

        txPrepare(5);
        txPrepare(5);
        txPrepare(5);
        txCommit(5);

        tracker.writeLockState();

        IgniteInternalFuture<Map<GridCacheVersion, WALPointer>> fut;
        try {
            tracker.startTxFinishAwaiting(1_000, 10_000);

            fut = tracker.awaitPendingTxsFinished(Collections.emptySet());
        }
        finally {
            tracker.writeUnlockState();
        }

        Thread.sleep(100);

        txCommit(5);
        txCommit(2);
        txCommit(2);

        long curTs = U.currentTimeMillis();

        Map<GridCacheVersion, WALPointer> pendingTxs = fut.get();

        assertTrue("Waiting for awaitFinishOfPreparedTxs future too long", U.currentTimeMillis() - curTs < 1_000);

        assertEquals(3, pendingTxs.size());
        assertTrue(pendingTxs.keySet().contains(nearXidVersion(1)));
        assertTrue(pendingTxs.keySet().contains(nearXidVersion(3)));
        assertTrue(pendingTxs.keySet().contains(nearXidVersion(5)));

        txCommit(1);
        txCommit(3);
        txCommit(5);

        tracker.writeLockState();

        try {
            tracker.startTxFinishAwaiting(1_000, 10_000);

            fut = tracker.awaitPendingTxsFinished(Collections.emptySet());
        }
        finally {
            tracker.writeUnlockState();
        }

        assertTrue(fut.get().isEmpty());
    }

    /**
     *
     */
    @Test(timeout = 10_000)
    public void testAwaitFinishOfPreparedTxsTimeouts() throws Exception {
        txPrepare(1);
        txCommit(1);

        txPrepare(2);
        txKeyRead(2, 10);

        txPrepare(3);
        txKeyWrite(3, 11);

        txPrepare(4);

        long curTs, waitMs;

        Map<GridCacheVersion, WALPointer> pendingTxs;

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final CountDownLatch latch3 = new CountDownLatch(1);
        final CountDownLatch latch4 = new CountDownLatch(1);

        new Thread(() -> {
            try {
                latch1.countDown();

                latch2.await();

                txCommit(2);

                latch3.await();

                Thread.sleep(200);
                txCommit(3);

                latch4.await();

                Thread.sleep(200);
                txCommit(4);
            }
            catch (InterruptedException ignored) {
                // No-op
            }
        }).start();

        latch1.await();

        pendingTxs = awaitFinishOfPreparedTxs(100, 200);

        assertEquals(3, pendingTxs.size());
        assertTrue(pendingTxs.keySet().contains(nearXidVersion(2)));
        assertTrue(pendingTxs.keySet().contains(nearXidVersion(3)));
        assertTrue(pendingTxs.keySet().contains(nearXidVersion(4)));

        latch2.countDown();

        curTs = U.currentTimeMillis();

        pendingTxs = awaitFinishOfPreparedTxs(100, 200);

        waitMs = U.currentTimeMillis() - curTs;

        assertTrue("Waiting for awaitFinishOfPreparedTxs future too short: " + waitMs, waitMs > 200 - 50);
        assertTrue("Waiting for awaitFinishOfPreparedTxs future too long: " + waitMs, waitMs < 200 + 500);

        assertEquals(2, pendingTxs.size());
        assertTrue(pendingTxs.keySet().contains(nearXidVersion(3)));
        assertTrue(pendingTxs.keySet().contains(nearXidVersion(4)));

        latch3.countDown();

        curTs = U.currentTimeMillis();

        pendingTxs = awaitFinishOfPreparedTxs(100, 300);

        waitMs = U.currentTimeMillis() - curTs;

        assertTrue("Waiting for awaitFinishOfPreparedTxs future too short: " + waitMs, waitMs > 200 - 50);
        assertTrue("Waiting for awaitFinishOfPreparedTxs future too long: " + waitMs, waitMs < 200 + 500);

        assertEquals(1, pendingTxs.size());
        assertTrue(pendingTxs.keySet().contains(nearXidVersion(4)));

        latch4.countDown();

        curTs = U.currentTimeMillis();

        pendingTxs = awaitFinishOfPreparedTxs(300, 500);

        waitMs = U.currentTimeMillis() - curTs;

        assertTrue("Waiting for awaitFinishOfPreparedTxs future too short: " + waitMs, waitMs > 200 - 50);
        assertTrue("Waiting for awaitFinishOfPreparedTxs future too long: " + waitMs, waitMs < 200 + 500);

        assertTrue(pendingTxs.isEmpty());
    }

    /**
     *
     */
    @Test
    public void trackingCommittedTest() {
        txPrepare(1);
        txCommit(1);

        txPrepare(2);

        tracker.writeLockState();
        try {
            tracker.startTrackingCommitted();
        }
        finally {
            tracker.writeUnlockState();
        }

        txCommit(2);

        txPrepare(3);
        txCommit(3);

        txPrepare(4);

        tracker.writeLockState();

        Map<GridCacheVersion, WALPointer> committedTxs;
        try {
            committedTxs = tracker.stopTrackingCommitted().committedTxs();
        }
        finally {
            tracker.writeUnlockState();
        }

        assertEquals(2, committedTxs.size());
        assertTrue(committedTxs.containsKey(nearXidVersion(2)));
        assertTrue(committedTxs.containsKey(nearXidVersion(3)));
    }

    /**
     *
     */
    @Test
    public void trackingPreparedTest() {
        txPrepare(1);
        txCommit(1);

        txPrepare(2);

        tracker.writeLockState();
        try {
            tracker.startTrackingPrepared();
        }
        finally {
            tracker.writeUnlockState();
        }

        txCommit(2);

        txPrepare(3);
        txCommit(3);

        txPrepare(4);

        tracker.writeLockState();

        Map<GridCacheVersion, WALPointer> committedTxs;
        try {
            committedTxs = tracker.stopTrackingPrepared();
        }
        finally {
            tracker.writeUnlockState();
        }

        assertEquals(2, committedTxs.size());
        assertTrue(committedTxs.containsKey(nearXidVersion(3)));
        assertTrue(committedTxs.containsKey(nearXidVersion(4)));
    }

    /**
     *
     */
    @Test(timeout = 10_000)
    public void testConsistentCutUseCase() throws Exception {
        txPrepare(1);
        txPrepare(2);
        txPrepare(3);

        txCommit(3);

        tracker.writeLockState(); // Cut 1.

        IgniteInternalFuture<Map<GridCacheVersion, WALPointer>> awaitFutCut1;
        try {
            tracker.startTrackingCommitted();

            tracker.startTxFinishAwaiting(1_000, 10_000);

            awaitFutCut1 = tracker.awaitPendingTxsFinished(Collections.emptySet());
        }
        finally {
            tracker.writeUnlockState();
        }

        txCommit(1);

        Map<GridCacheVersion, WALPointer> failedToFinish = awaitFutCut1.get();

        assertEquals(1, failedToFinish.size());
        assertTrue(failedToFinish.keySet().contains(nearXidVersion(2)));

        txCommit(2);

        txPrepare(4);
        txCommit(4);

        txPrepare(5);

        txPrepare(6);

        tracker.writeLockState(); // Cut 2.

        Map<GridCacheVersion, WALPointer> committedFrom1to2;
        Map<GridCacheVersion, WALPointer> preparedOn2;
        try {
            committedFrom1to2 = tracker.stopTrackingCommitted().committedTxs();

            preparedOn2 = tracker.currentlyPreparedTxs();

            tracker.startTrackingPrepared();
        }
        finally {
            tracker.writeUnlockState();
        }

        assertEquals(2, preparedOn2.size());
        assertTrue(preparedOn2.keySet().contains(nearXidVersion(5)));
        assertTrue(preparedOn2.keySet().contains(nearXidVersion(6)));

        assertEquals(3, committedFrom1to2.size());
        assertTrue(committedFrom1to2.keySet().contains(nearXidVersion(1)));
        assertTrue(committedFrom1to2.keySet().contains(nearXidVersion(2)));
        assertTrue(committedFrom1to2.keySet().contains(nearXidVersion(4)));

        txPrepare(7);
        txPrepare(8);

        txCommit(6);
        txCommit(7);

        tracker.writeLockState(); // Cut 3.
        Map<GridCacheVersion, WALPointer> preparedFrom2to3;
        try {
            preparedFrom2to3 = tracker.stopTrackingPrepared();
        }
        finally {
            tracker.writeUnlockState();
        }

        assertEquals(2, preparedFrom2to3.size());
        assertTrue(preparedFrom2to3.keySet().contains(nearXidVersion(7)));
        assertTrue(preparedFrom2to3.keySet().contains(nearXidVersion(8)));
    }

    /**
     *
     */
    @Test
    public void testDependentTransactions() {
        tracker.writeLockState();
        try {
            tracker.startTrackingCommitted();
        }
        finally {
            tracker.writeUnlockState();
        }

        txPrepare(1);
        txKeyRead(1, 0);
        txKeyWrite(1, 10);
        txKeyRead(1, 20);
        txCommit(1);

        txPrepare(2);
        txKeyWrite(2, 30);
        txKeyWrite(2, 40);
        txCommit(2);

        txPrepare(3);
        txKeyRead(3, 10); // (w -> r) is a dependency
        txCommit(3);

        txPrepare(4);
        txKeyWrite(4, 20); // (r -> w) is not a dependency
        txCommit(4);

        txPrepare(5);
        txKeyRead(5, 30); // (w -> r) is a dependency
        txCommit(5);

        txPrepare(6);
        txKeyWrite(6, 40); // (w -> w) is a dependency
        txCommit(6);

        txPrepare(7);
        txKeyRead(7, 0); // (r -> r) is not a dependency
        txCommit(7);

        tracker.writeLockState();

        TrackCommittedResult res;
        try {
            res = tracker.stopTrackingCommitted();
        }
        finally {
            tracker.writeUnlockState();
        }

        assertEquals(7, res.committedTxs().size());
        assertEquals(2, res.dependentTxsGraph().size());

        assertTrue(res.dependentTxsGraph().containsKey(nearXidVersion(1)));
        assertTrue(res.dependentTxsGraph().containsKey(nearXidVersion(2)));

        Set<GridCacheVersion> dependentFrom1 = res.dependentTxsGraph().get(nearXidVersion(1));
        assertEquals(1, dependentFrom1.size());
        assertTrue(dependentFrom1.contains(nearXidVersion(3)));

        Set<GridCacheVersion> dependentFrom2 = res.dependentTxsGraph().get(nearXidVersion(2));
        assertEquals(2, dependentFrom2.size());
        assertTrue(dependentFrom2.contains(nearXidVersion(5)));
        assertTrue(dependentFrom2.contains(nearXidVersion(6)));
    }

    /**
     * Transaction tracker memory leak test.
     */
    @Test
    public void testTrackerMemoryLeak() throws Exception {
        int allowedLeakSize = 100 * 1024;

        // Warmup phase.
        long sizeBefore = memoryFootprintForTransactionTracker(1000, 20);

        // Main phase.
        long sizeAfter = memoryFootprintForTransactionTracker(5000, 20);

        assertTrue("Possible memory leak detected. Memory consumed before transaction tracking: " + sizeBefore +
            ", memory consumed after transaction tracking: " + sizeAfter, sizeAfter - sizeBefore < allowedLeakSize);
    }

    /**
     * @param iterationsCnt Iterations count.
     * @param threadsCnt Threads count.
     */
    private long memoryFootprintForTransactionTracker(int iterationsCnt, int threadsCnt) throws Exception {
        AtomicInteger txCnt = new AtomicInteger();

        AtomicInteger trackerState = new AtomicInteger();

        File dumpFile = new File(U.defaultWorkDirectory(), "test.hprof");

        String heapDumpFileName = dumpFile.getAbsolutePath();

        Runnable txRunnable = new Runnable() {
            @Override public void run() {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int iteration = 0; iteration < iterationsCnt; iteration++) {
                    int txId = txCnt.incrementAndGet();

                    txPrepare(txId);

                    int opCnt = rnd.nextInt(100);

                    for (int i = 0; i < opCnt; i++) {
                        if (rnd.nextBoolean())
                            txKeyRead(txId, rnd.nextInt());
                        else
                            txKeyWrite(txId, rnd.nextInt());
                    }

                    if (rnd.nextInt(10) == 0)
                        txRollback(txId);
                    else
                        txCommit(txId);

                    // Change tracker state
                    if (rnd.nextInt(20) == 0) {
                        tracker.writeLockState();

                        try {
                            int state = trackerState.getAndIncrement();

                            switch (state % 4) {
                                case 0:
                                    tracker.startTrackingPrepared();

                                    break;
                                case 1:
                                    tracker.stopTrackingPrepared();

                                    break;
                                case 2:
                                    tracker.startTrackingCommitted();

                                    break;
                                case 3:
                                    tracker.stopTrackingCommitted();
                            }
                        }
                        finally {
                            tracker.writeUnlockState();
                        }
                    }
                }
            }
        };

        GridTestUtils.runMultiThreaded(txRunnable, threadsCnt, "tx-runner");

        tracker.writeLockState();

        try {
            tracker.stopTrackingPrepared();
            tracker.stopTrackingCommitted();
        }
        finally {
            tracker.writeUnlockState();
        }

        GridDebug.dumpHeap(heapDumpFileName, true);

        long fileSize = dumpFile.length();

        dumpFile.delete();

        return fileSize;
    }

    /**
     * @param txId Test transaction ID.
     */
    private void txPrepare(int txId) {
        tracker.onTxPrepared(nearXidVersion(txId), new FileWALPointer(0, txId * 10, 1));
    }

    /**
     * @param txId Test transaction ID.
     */
    private void txCommit(int txId) {
        tracker.onTxCommitted(nearXidVersion(txId));
    }

    /**
     * @param txId Test transaction ID.
     */
    private void txRollback(int txId) {
        tracker.onTxRolledBack(nearXidVersion(txId));
    }

    /**
     * @param txId Test transaction ID.
     * @param key Key.
     */
    private void txKeyWrite(int txId, int key) {
        KeyCacheObjectImpl keyCacheObj = new KeyCacheObjectImpl(key, ByteBuffer.allocate(4).putInt(key).array(), 1);

        tracker.onKeysWritten(nearXidVersion(txId), Collections.singletonList(keyCacheObj));
    }

    /**
     * @param txId Test transaction ID.
     * @param key Key.
     */
    private void txKeyRead(int txId, int key) {
        KeyCacheObjectImpl keyCacheObj = new KeyCacheObjectImpl(key, ByteBuffer.allocate(4).putInt(key).array(), 1);

        tracker.onKeysRead(nearXidVersion(txId), Collections.singletonList(keyCacheObj));
    }

    /**
     * @param txId Test transaction ID.
     */
    private GridCacheVersion nearXidVersion(int txId) {
        return new GridCacheVersion(0, txId, 0);
    }

    /**
     * @param preparedTxsTimeout Prepared transactions timeout.
     * @param committingTxsTimeout Committing transactions timeout.
     */
    private Map<GridCacheVersion, WALPointer> awaitFinishOfPreparedTxs(long preparedTxsTimeout,
        long committingTxsTimeout) throws IgniteCheckedException {
        IgniteInternalFuture<Map<GridCacheVersion, WALPointer>> fut;

        tracker.writeLockState();

        try {
            tracker.startTxFinishAwaiting(preparedTxsTimeout, committingTxsTimeout);

            fut = tracker.awaitPendingTxsFinished(Collections.emptySet());
        }
        finally {
            tracker.writeUnlockState();
        }

        return fut.get();
    }
}
