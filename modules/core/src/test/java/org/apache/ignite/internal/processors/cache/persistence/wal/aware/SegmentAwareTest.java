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
import junit.framework.TestCase;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.GridTestUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test for {@link SegmentAware}.
 */
public class SegmentAwareTest extends TestCase {

    /**
     * Checking to avoid deadlock SegmentArchivedStorage.markAsMovedToArchive -> SegmentLockStorage.locked <->
     * SegmentLockStorage.releaseWorkSegment -> SegmentArchivedStorage.onSegmentUnlocked
     *
     * @throws IgniteCheckedException if failed.
     */
    public void testAvoidDeadlockArchiverAndLockStorage() throws IgniteCheckedException {
        SegmentAware aware = new SegmentAware(10, false);

        int iterationCnt = 100_000;
        int segmentToHandle = 1;

        IgniteInternalFuture archiverThread = GridTestUtils.runAsync(() -> {
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

        IgniteInternalFuture lockerThread = GridTestUtils.runAsync(() -> {
            int i = iterationCnt;

            while (i-- > 0) {
                aware.lockWorkSegment(segmentToHandle);

                aware.releaseWorkSegment(segmentToHandle);
            }
        });

        archiverThread.get();
        lockerThread.get();
    }

    /**
     * Waiting finished when work segment is set.
     */
    public void testFinishAwaitSegment_WhenExactWaitingSegmentWasSet() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegment(5));

        //when: set exact awaiting segment.
        aware.curAbsWalIdx(5);

        //then: waiting should finish immediately
        future.get(20);
    }

    /**
     * Waiting finished when work segment greater than expected is set.
     */
    public void testFinishAwaitSegment_WhenGreaterThanWaitingSegmentWasSet() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegment(5));

        //when: set grater than awaiting segment.
        aware.curAbsWalIdx(10);

        //then: waiting should finish immediately
        future.get(20);
    }

    /**
     * Waiting finished when work segment is set.
     */
    public void testFinishAwaitSegment_WhenNextSegmentEqualToWaitingOne() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

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
    public void testFinishAwaitSegment_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegment(5));

        //when: interrupt waiting.
        aware.interrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertTrue(future.get(20) instanceof IgniteInterruptedCheckedException);
    }

    /**
     * Waiting finished when next work segment triggered.
     */
    public void testFinishWaitSegmentForArchive_WhenWorkSegmentIncremented() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

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
    public void testFinishWaitSegmentForArchive_WhenWorkSegmentGreaterValue() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

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
    public void testFinishWaitSegmentForArchive_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

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
    public void testCorrectCalculateNextSegmentIndex() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        aware.curAbsWalIdx(5);

        //when: request next work segment.
        long segmentIndex = aware.nextAbsoluteSegmentIndex();

        //then:
        assertThat(segmentIndex, is(6L));
    }

    /**
     * Waiting finished when segment archived.
     */
    public void testFinishWaitNextAbsoluteIndex_WhenMarkAsArchivedFirstSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(2, false);

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
    public void testFinishWaitNextAbsoluteIndex_WhenSetToArchivedFirst() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(2, false);

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
    public void testFinishWaitNextAbsoluteIndex_WhenOnlyForceInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(2, false);

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
    public void testFinishSegmentArchived_WhenSetExactWaitingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: archived exact expected segment.
        aware.setLastArchivedAbsoluteIndex(5);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when segment archived.
     */
    public void testFinishSegmentArchived_WhenMarkExactWaitingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: mark exact segment as moved.
        aware.markAsMovedToArchive(5);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when segment archived.
     */
    public void testFinishSegmentArchived_WhenSetGreaterThanWaitingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: archived greater than expected segment.
        aware.setLastArchivedAbsoluteIndex(7);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when segment archived.
     */
    public void testFinishSegmentArchived_WhenMarkGreaterThanWaitingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: moved greater than expected segment.
        aware.markAsMovedToArchive(7);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished when interrupt was triggered.
     */
    public void testFinishSegmentArchived_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

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
    public void testMarkAsMovedToArchive_WhenReleaseLockedSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        aware.checkCanReadArchiveOrReserveWorkSegment(5);

        IgniteInternalFuture future = awaitThread(() -> aware.markAsMovedToArchive(5));

        //when: release exact expected work segment.
        aware.releaseWorkSegment(5);

        //then: waiting should finish immediately.
        future.get(20);
    }

    /**
     * Waiting finished and increment archived segment when interrupt was call.
     */
    public void testMarkAsMovedToArchive_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);
        aware.checkCanReadArchiveOrReserveWorkSegment(5);

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
    public void testFinishWaitSegmentToCompress_WhenSetLastArchivedSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, true);

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
    public void testFinishWaitSegmentToCompress_WhenMarkLastArchivedSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, true);

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
    public void testCorrectCalculateNextCompressSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, true);

        aware.onSegmentCompressed(5);
        aware.setLastArchivedAbsoluteIndex(6);
        aware.lastTruncatedArchiveIdx(7);

        //when:
        long segmentToCompress = aware.waitNextSegmentToCompress();

        //then: segment to compress greater than truncated archive idx
        assertEquals(8, segmentToCompress);
    }

    /**
     * Waiting finished when interrupt was call.
     */
    public void testFinishWaitSegmentToCompress_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, true);
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
    public void testLastCompressedIdxProperOrdering() throws IgniteInterruptedCheckedException {
        SegmentAware aware = new SegmentAware(10, true);

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
    public void testReserveCorrectly() {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

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
     * Should fail when release unreserved segment.
     */
    public void testAssertFail_WhenReleaseUnreservedSegment() {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        aware.reserve(5);
        try {

            aware.release(7);
        }
        catch (AssertionError e) {
            return;
        }

        fail("Should fail with AssertError because this segment have not reserved");
    }

    /**
     * Segment locked correctly.
     */
    public void testReserveWorkSegmentCorrectly() {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        //when: lock one segment twice.
        aware.checkCanReadArchiveOrReserveWorkSegment(5);
        aware.checkCanReadArchiveOrReserveWorkSegment(5);

        //then: exact one segment should locked.
        assertTrue(aware.locked(5));
        assertFalse(aware.locked(6));
        assertFalse(aware.locked(4));

        //when: release segment once.
        aware.releaseWorkSegment(5);

        //then: nothing to change, segment still locked.
        assertTrue(aware.locked(5));
        assertFalse(aware.locked(6));
        assertFalse(aware.locked(4));

        //when: release segment.
        aware.releaseWorkSegment(5);

        //then: all segments should be unlocked.
        assertFalse(aware.locked(5));
        assertFalse(aware.locked(6));
        assertFalse(aware.locked(4));
    }

    /**
     * Should fail when release unlocked segment.
     */
    public void testAssertFail_WhenReleaseUnreservedWorkSegment() {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10, false);

        aware.checkCanReadArchiveOrReserveWorkSegment(5);
        try {

            aware.releaseWorkSegment(7);
        }
        catch (AssertionError e) {
            return;
        }

        fail("Should fail with AssertError because this segment have not reserved");
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
        IgniteInternalFuture<Object> future = GridTestUtils.runAsync(
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
}