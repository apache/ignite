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
package org.apache.ignite.internal.processors.cache.persistence.wal.aware;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.logger.NullLogger;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link SegmentAware}.
 */
public class SegmentAwareTest {
    /**
     * Checking to avoid deadlock SegmentArchivedStorage.markAsMovedToArchive -> SegmentLockStorage.locked <->
     * SegmentLockStorage.releaseWorkSegment -> SegmentArchivedStorage.onSegmentUnlocked
     *
     * @throws IgniteCheckedException if failed.
     */
    @Test
    public void testAvoidDeadlockArchiverAndLockStorage() throws IgniteCheckedException {
        SegmentAware aware = segmentAware(10);

        int iterationCnt = 100_000;
        int segmentToHandle = 1;

        IgniteInternalFuture archiverThread = runAsync(() -> {
            int i = iterationCnt;

            while (i-- > 0) {
                try {
                    aware.markAsMovedToArchive(segmentToHandle);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        IgniteInternalFuture lockerThread = runAsync(() -> {
            int i = iterationCnt;

            while (i-- > 0) {
                if (aware.lock(segmentToHandle))
                    aware.unlock(segmentToHandle);
            }
        });

        archiverThread.get();
        lockerThread.get();
    }

    /**
     * Waiting finished when work segment is set.
     */
    @Test
    public void testFinishAwaitSegment_WhenExactWaitingSegmentWasSet() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegment(5));

        //when: set exact awaiting segment.
        aware.curAbsWalIdx(5);

        //then: waiting should finish immediately
        future.get(20);
    }

    /**
     * Waiting finished when work segment greater than expected is set.
     */
    @Test
    public void testFinishAwaitSegment_WhenGreaterThanWaitingSegmentWasSet() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegment(5));

        //when: set grater than awaiting segment.
        aware.curAbsWalIdx(10);

        //then: waiting should finish immediately
        future.get(20);
    }

    /**
     * Waiting finished when work segment is set.
     */
    @Test
    public void testFinishAwaitSegment_WhenNextSegmentEqualToWaitingOne() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegment(5));

        //when: set less than awaiting segment.
        aware.curAbsWalIdx(4);

        //then: thread still waiting the segment
        assertFutureIsNotFinish(future);

        //when: trigger next segment.
        aware.nextAbsoluteSegmentIndex();

        //then: waiting should finish immediately
        future.get(20);
    }

    /**
     * Waiting finished when interrupt was triggered.
     */
    @Test
    public void testFinishAwaitSegment_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegment(5));

        //when: interrupt waiting.
        aware.interrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertTrue(future.get(20) instanceof IgniteInterruptedCheckedException);
    }

    /**
     * Waiting finished when next work segment triggered.
     */
    @Test
    public void testFinishWaitSegmentForArchive_WhenWorkSegmentIncremented() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        aware.curAbsWalIdx(5);
        aware.setLastArchivedAbsoluteIndex(4);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentForArchivation);

        //when: next work segment triggered.
        aware.nextAbsoluteSegmentIndex();

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when work segment is set.
     */
    @Test
    public void testFinishWaitSegmentForArchive_WhenWorkSegmentGreaterValue() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        aware.curAbsWalIdx(5);
        aware.setLastArchivedAbsoluteIndex(4);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentForArchivation);

        //when: set work segment greater than required.
        aware.curAbsWalIdx(7);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when interrupt was triggered.
     */
    @Test
    public void testFinishWaitSegmentForArchive_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        aware.curAbsWalIdx(5);
        aware.setLastArchivedAbsoluteIndex(4);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentForArchivation);

        //when: interrupt waiting.
        aware.interrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertTrue(future.get(20) instanceof IgniteInterruptedCheckedException);
    }

    /**
     * Should correct calculate next segment.
     */
    @Test
    public void testCorrectCalculateNextSegmentIndex() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        aware.curAbsWalIdx(5);

        //when: request next work segment.
        long segmentIndex = aware.nextAbsoluteSegmentIndex();

        //then:
        assertThat(segmentIndex, is(6L));
    }

    /**
     * Waiting finished when segment archived.
     */
    @Test
    public void testFinishWaitNextAbsoluteIndex_WhenMarkAsArchivedFirstSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(2);

        aware.curAbsWalIdx(1);
        aware.setLastArchivedAbsoluteIndex(-1);

        IgniteInternalFuture future = awaitThread(aware::nextAbsoluteSegmentIndex);

        //when: mark first segment as moved.
        aware.markAsMovedToArchive(0);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when segment archived.
     */
    @Test
    public void testFinishWaitNextAbsoluteIndex_WhenSetToArchivedFirst() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(2);

        aware.curAbsWalIdx(1);
        aware.setLastArchivedAbsoluteIndex(-1);

        IgniteInternalFuture future = awaitThread(aware::nextAbsoluteSegmentIndex);

        //when: mark first segment as moved.
        aware.setLastArchivedAbsoluteIndex(0);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when force interrupt was triggered.
     */
    @Test
    public void testFinishWaitNextAbsoluteIndex_WhenOnlyForceInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(2);

        aware.curAbsWalIdx(2);
        aware.setLastArchivedAbsoluteIndex(-1);

        IgniteInternalFuture future = awaitThread(aware::nextAbsoluteSegmentIndex);

        //when: interrupt waiting.
        aware.interrupt();

        //then: nothing to happen because nextAbsoluteSegmentIndex is not interrupt by "interrupt" call.
        assertFutureIsNotFinish(future);

        //when: force interrupt waiting.
        aware.forceInterrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertTrue(future.get(20) instanceof IgniteInterruptedCheckedException);
    }

    /**
     * Waiting finished when segment archived.
     */
    @Test
    public void testFinishSegmentArchived_WhenSetExactWaitingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: archived exact expected segment.
        aware.setLastArchivedAbsoluteIndex(5);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when segment archived.
     */
    @Test
    public void testFinishSegmentArchived_WhenMarkExactWaitingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: mark exact segment as moved.
        aware.markAsMovedToArchive(5);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when segment archived.
     */
    @Test
    public void testFinishSegmentArchived_WhenSetGreaterThanWaitingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: archived greater than expected segment.
        aware.setLastArchivedAbsoluteIndex(7);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when segment archived.
     */
    @Test
    public void testFinishSegmentArchived_WhenMarkGreaterThanWaitingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: moved greater than expected segment.
        aware.markAsMovedToArchive(7);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when interrupt was triggered.
     */
    @Test
    public void testFinishSegmentArchived_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        aware.curAbsWalIdx(5);
        aware.setLastArchivedAbsoluteIndex(4);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: interrupt waiting.
        aware.interrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertTrue(future.get(20) instanceof IgniteInterruptedCheckedException);
    }

    /**
     * Waiting finished when release work segment.
     */
    @Test
    public void testMarkAsMovedToArchive_WhenReleaseLockedSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        assertTrue(aware.lock(5));

        IgniteInternalFuture future = awaitThread(() -> aware.markAsMovedToArchive(5));

        //when: release exact expected work segment.
        aware.unlock(5);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished and increment archived segment when interrupt was call.
     */
    @Test
    public void testMarkAsMovedToArchive_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        assertTrue(aware.lock(5));

        IgniteInternalFuture future = awaitThread(() -> aware.markAsMovedToArchive(5));

        //when: interrupt waiting.
        aware.interrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertFalse(future.get(20) instanceof IgniteInterruptedCheckedException);

        //and: last archived segment should be changed.
        assertEquals(5, aware.lastArchivedAbsoluteIndex());
    }

    /**
     * Waiting finished when segment archived.
     */
    @Test
    public void testFinishWaitSegmentToCompress_WhenSetLastArchivedSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10, true);

        aware.onSegmentCompressed(5);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentToCompress);

        //when: archived expected segment.
        aware.setLastArchivedAbsoluteIndex(6);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when segment archived.
     */
    @Test
    public void testFinishWaitSegmentToCompress_WhenMarkLastArchivedSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10, true);

        aware.onSegmentCompressed(5);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentToCompress);

        //when: marked expected segment.
        aware.markAsMovedToArchive(6);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Next segment for compress based on truncated archive idx.
     */
    @Test
    public void testCorrectCalculateNextCompressSegment() throws IgniteCheckedException, InterruptedException {
        SegmentAware aware = segmentAware(10, true);

        aware.setLastArchivedAbsoluteIndex(6);

        for (int exp = 0; exp <= 6; exp++)
            assertEquals(exp, aware.waitNextSegmentToCompress());
    }

    /**
     * Waiting finished when interrupt was call.
     */
    @Test
    public void testFinishWaitSegmentToCompress_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10, true);
        aware.onSegmentCompressed(5);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentToCompress);

        //when: interrupt waiting.
        aware.interrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertTrue(future.get(20) instanceof IgniteInterruptedCheckedException);
    }

    /**
     * Tests that {@link SegmentAware#onSegmentCompressed} returns segments in proper order.
     */
    @Test
    public void testLastCompressedIdxProperOrdering() throws IgniteInterruptedCheckedException {
        SegmentAware aware = segmentAware(10, true);

        for (int i = 0; i < 5; i++) {
            aware.setLastArchivedAbsoluteIndex(i);
            aware.waitNextSegmentToCompress();
        }

        aware.onSegmentCompressed(0);

        aware.onSegmentCompressed(4);
        assertEquals(0, aware.lastCompressedIdx());
        aware.onSegmentCompressed(1);
        assertEquals(1, aware.lastCompressedIdx());
        aware.onSegmentCompressed(3);
        assertEquals(1, aware.lastCompressedIdx());
        aware.onSegmentCompressed(2);
        assertEquals(4, aware.lastCompressedIdx());
    }

    /**
     * Segment reserve correctly.
     */
    @Test
    public void testReserveCorrectly() {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        // Set limits.
        aware.curAbsWalIdx(10);
        aware.minReserveIndex(0);

        //when: reserve one segment twice and one segment once.
        aware.reserve(5);
        aware.reserve(5);
        aware.reserve(7);

        //then: segments greater than minimum should be reserved.
        assertTrue(aware.reserved(5));
        assertTrue(aware.reserved(10));
        assertFalse(aware.reserved(4));

        //when: release one of twice locked segment.
        aware.release(5);

        //then: nothing to change.
        assertTrue(aware.reserved(5));
        assertTrue(aware.reserved(10));
        assertFalse(aware.reserved(4));

        //when: again release one of twice locked segment.
        aware.release(5);

        //then: segments greater than second locked segment should be reserved.
        assertTrue(aware.reserved(7));
        assertTrue(aware.reserved(10));
        assertFalse(aware.reserved(5));
        assertFalse(aware.reserved(6));

        //when: release last segment.
        aware.release(7);

        //then: all segments should be released.
        assertFalse(aware.reserved(7));
        assertFalse(aware.reserved(10));
        assertFalse(aware.reserved(6));
    }

    /**
     * Check that there will be no error if a non-reserved segment is released.
     */
    @Test
    public void testReleaseUnreservedSegment() {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        aware.reserve(5);
        aware.release(7);
    }

    /**
     * Segment locked correctly.
     */
    @Test
    public void testReserveWorkSegmentCorrectly() {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        //when: lock one segment twice.
        assertTrue(aware.lock(5));
        assertTrue(aware.lock(5));

        //then: exact one segment should locked.
        assertTrue(aware.locked(5));
        assertFalse(aware.locked(6));
        assertFalse(aware.locked(4));

        //when: release segment once.
        aware.unlock(5);

        //then: nothing to change, segment still locked.
        assertTrue(aware.locked(5));
        assertFalse(aware.locked(6));
        assertFalse(aware.locked(4));

        //when: release segment.
        aware.unlock(5);

        //then: all segments should be unlocked.
        assertFalse(aware.locked(5));
        assertFalse(aware.locked(6));
        assertFalse(aware.locked(4));
    }

    /**
     * Should fail when release unlocked segment.
     */
    @Test
    public void testAssertFail_WhenReleaseUnreservedWorkSegment() {
        //given: thread which awaited segment.
        SegmentAware aware = segmentAware(10);

        assertTrue(aware.lock(5));
        try {
            aware.unlock(7);
        }
        catch (AssertionError e) {
            return;
        }

        fail("Should fail with AssertError because this segment have not reserved");
    }

    /**
     * Check that the reservation border is working correctly.
     */
    @Test
    public void testReservationBorder() {
        SegmentAware aware = segmentAware(10);

        assertTrue(aware.reserve(0));
        assertTrue(aware.reserve(1));

        assertFalse(aware.minReserveIndex(0));
        assertFalse(aware.minReserveIndex(1));

        aware.release(0);

        assertTrue(aware.minReserveIndex(0));
        assertFalse(aware.minReserveIndex(1));

        assertFalse(aware.reserve(0));
        assertTrue(aware.reserve(1));
    }

    /**
     * Check that the lock border is working correctly.
     */
    @Test
    public void testLockBorder() {
        SegmentAware aware = segmentAware(10);

        assertTrue(aware.lock(0));
        assertTrue(aware.lock(1));

        assertFalse(aware.minLockIndex(0));
        assertFalse(aware.minLockIndex(1));

        aware.unlock(0);

        assertTrue(aware.minLockIndex(0));
        assertFalse(aware.minLockIndex(1));

        assertFalse(aware.lock(0));
        assertTrue(aware.lock(1));
    }

    /**
     * Checking the correctness of WAL archive size.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWalArchiveSize() throws Exception {
        SegmentAware aware = segmentAware(10);

        IgniteInternalFuture<?> fut = awaitThread(() -> aware.awaitExceedMaxArchiveSize(10));

        aware.addSize(0, 4, 0);
        assertFutureIsNotFinish(fut);

        aware.addSize(0, 0, 4);
        assertFutureIsNotFinish(fut);

        aware.addSize(0, 4, 0);
        fut.get(20);

        aware.resetWalArchiveSizes();

        fut = awaitThread(() -> aware.awaitExceedMaxArchiveSize(10));

        aware.addSize(1, 4, 0);
        assertFutureIsNotFinish(fut);

        aware.addSize(1, 0, 4);
        assertFutureIsNotFinish(fut);

        aware.addSize(1, 0, 4);
        fut.get(20);

        aware.resetWalArchiveSizes();

        fut = awaitThread(() -> aware.awaitExceedMaxArchiveSize(10));

        aware.interrupt();
        assertTrue(fut.get(20) instanceof IgniteInterruptedCheckedException);

        aware.reset();

        fut = awaitThread(() -> aware.awaitExceedMaxArchiveSize(10));

        aware.forceInterrupt();
        assertTrue(fut.get(20) instanceof IgniteInterruptedCheckedException);
    }

    /**
     * Checking the correctness of truncate logic.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTruncate() throws Exception {
        SegmentAware aware = segmentAware(10);

        IgniteInternalFuture<?> fut = awaitThread(aware::awaitAvailableTruncateArchive);

        aware.lastCheckpointIdx(5);

        fut.get(20);
        assertEquals(5, aware.awaitAvailableTruncateArchive());

        aware.reserve(4);
        assertEquals(4, aware.awaitAvailableTruncateArchive());

        aware.setLastArchivedAbsoluteIndex(3);
        assertEquals(3, aware.awaitAvailableTruncateArchive());

        aware.lastTruncatedArchiveIdx(0);
        assertEquals(2, aware.awaitAvailableTruncateArchive());
        assertEquals(0, aware.lastTruncatedArchiveIdx());

        aware.reserve(0);
        fut = awaitThread(aware::awaitAvailableTruncateArchive);

        aware.release(0);

        fut.get(20);
        assertEquals(2, aware.awaitAvailableTruncateArchive());

        aware.setLastArchivedAbsoluteIndex(4);
        assertEquals(3, aware.awaitAvailableTruncateArchive());

        aware.release(4);
        assertEquals(3, aware.awaitAvailableTruncateArchive());

        aware.lastCheckpointIdx(6);
        assertEquals(3, aware.awaitAvailableTruncateArchive());

        aware.setLastArchivedAbsoluteIndex(6);
        assertEquals(5, aware.awaitAvailableTruncateArchive());
    }

    /**
     * Checking the correct creation {@link SegmentArchiveSizeStorage}.
     */
    @Test
    public void testCreateSegmentArchiveSizeStorage() {
        assertThrowsOnCreateSegmentArchiveSizeStorage(UNLIMITED_WAL_ARCHIVE, 100);
        assertThrowsOnCreateSegmentArchiveSizeStorage(200, 100);

        segmentAware(1, false, UNLIMITED_WAL_ARCHIVE, UNLIMITED_WAL_ARCHIVE);
        segmentAware(1, false, 100, UNLIMITED_WAL_ARCHIVE);
        segmentAware(1, false, 100, 200);
    }

    /**
     * Assert that future is still not finished.
     *
     * @param future Future to check.
     */
    private void assertFutureIsNotFinish(IgniteInternalFuture future) throws IgniteCheckedException {
        try {
            future.get(20);

            fail("Timeout should be appear because thread should be still work");
        }
        catch (IgniteFutureTimeoutCheckedException ignore) {

        }
    }

    /**
     * Create thread for execute waiter.
     *
     * @param waiter Waiter for execute in new thread.
     * @return Future of thread.
     */
    private IgniteInternalFuture awaitThread(Waiter waiter) throws IgniteCheckedException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        IgniteInternalFuture<Object> future = runAsync(
            () -> {
                latch.countDown();
                try {
                    waiter.await();
                }
                catch (IgniteInterruptedCheckedException e) {
                    return e;
                }

                return null;
            }
        );

        latch.await();

        assertFutureIsNotFinish(future);

        return future;
    }

    /**
     * Represent of command for waiting.
     */
    interface Waiter {
        /**
         * Some waiting operation.
         */
        void await() throws IgniteInterruptedCheckedException;
    }

    /**
     * Factory method for the {@link SegmentAware}.
     *
     * @param walSegmentsCnt Total WAL segments count.
     * @return New instance.
     */
    private SegmentAware segmentAware(int walSegmentsCnt) {
        return segmentAware(walSegmentsCnt, false);
    }

    /**
     * Factory method for the {@link SegmentAware}.
     *
     * @param walSegmentsCnt Total WAL segments count.
     * @param compactionEnabled Is wal compaction enabled.
     * @return New instance.
     */
    private SegmentAware segmentAware(int walSegmentsCnt, boolean compactionEnabled) {
        return segmentAware(walSegmentsCnt, compactionEnabled, UNLIMITED_WAL_ARCHIVE, UNLIMITED_WAL_ARCHIVE);
    }

    /**
     * Factory method for the {@link SegmentAware}.
     *
     * @param walSegmentsCnt Total WAL segments count.
     * @param compactionEnabled Is wal compaction enabled.
     * @param minWalArchiveSize Minimum size of the WAL archive in bytes
     *      or {@link DataStorageConfiguration#UNLIMITED_WAL_ARCHIVE}.
     * @param maxWalArchiveSize Maximum size of the WAL archive in bytes
     *      or {@link DataStorageConfiguration#UNLIMITED_WAL_ARCHIVE}.
     * @return New instance.
     */
    private SegmentAware segmentAware(
        int walSegmentsCnt,
        boolean compactionEnabled,
        long minWalArchiveSize,
        long maxWalArchiveSize
    ) {
        return new SegmentAware(
            new NullLogger(),
            walSegmentsCnt,
            compactionEnabled,
            minWalArchiveSize,
            maxWalArchiveSize
        );
    }

    /**
     * Checking that an exception will be thrown on creation {@link SegmentArchiveSizeStorage}.
     *
     * @param minWalArchiveSize Minimum size of the WAL archive in bytes
     *      or {@link DataStorageConfiguration#UNLIMITED_WAL_ARCHIVE}.
     * @param maxWalArchiveSize Maximum size of the WAL archive in bytes
     *      or {@link DataStorageConfiguration#UNLIMITED_WAL_ARCHIVE}.
     */
    private void assertThrowsOnCreateSegmentArchiveSizeStorage(long minWalArchiveSize, long maxWalArchiveSize) {
        assertThrows(
            null,
            () -> segmentAware(1, false, minWalArchiveSize, maxWalArchiveSize),
            AssertionError.class,
            null
        );
    }

    /**
     * Getting {@code SegmentAware#archiveSizeStorage}.
     *
     * @param aware Segment aware.
     * @return Instance of {@link SegmentArchiveSizeStorage}.
     */
    private SegmentArchiveSizeStorage archiveSizeStorage(SegmentAware aware) {
        return getFieldValue(aware, "archiveSizeStorage");
    }
}
