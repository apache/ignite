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
import com.google.common.base.CharMatcher;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.GridTestUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.Matchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SegmentAwareTest {

    @Test
    public void sholdFinishAwaitSegment_WhenExactWaitingSegmentWasSet() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegment(5));

        //when: set exact awaiting segment.
        aware.curAbsWalIdx(5);

        //then: waiting should finish immediately
        future.get(10);
    }

    @Test
    public void sholdFinishAwaitSegment_WhenGreaterThanWaitingSegmentWasSet() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegment(5));

        //when: set grater than awaiting segment.
        aware.curAbsWalIdx(10);

        //then: waiting should finish immediately
        future.get(10);
    }

    @Test
    public void sholdFinishAwaitSegment_WhenNextSegmentEqualToWaitingOne() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegment(5));

        //when: set less than awaiting segment.
        aware.curAbsWalIdx(4);

        //then: thread still waiting the segment
        assertFutureIsNotFinish(future);

        //when: trigger next segment.
        aware.nextAbsoluteSegmentIndex();

        //then: waiting should finish immediately
        future.get(10);
    }

    @Test
    public void sholdFinishAwaitSegment_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegment(5));

        //when: interrupt waiting.
        aware.interrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertTrue(future.get(10) instanceof IgniteInterruptedCheckedException);
    }

    @Test
    public void sholdFinishWaitSegmentForArchive_WhenWorkSegmentIncremented() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.curAbsWalIdx(5);
        aware.setLastArchivedAbsoluteIndex(4);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentForArchivation);

        //when: interrupt waiting.
        aware.nextAbsoluteSegmentIndex();

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdFinishWaitSegmentForArchive_WhenWorkSegmentGreaterValue() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.curAbsWalIdx(5);
        aware.setLastArchivedAbsoluteIndex(4);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentForArchivation);

        //when: interrupt waiting.
        aware.curAbsWalIdx(7);

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdFinishWaitSegmentForArchive_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.curAbsWalIdx(5);
        aware.setLastArchivedAbsoluteIndex(4);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentForArchivation);

        //when: interrupt waiting.
        aware.interrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertTrue(future.get(10) instanceof IgniteInterruptedCheckedException);
    }

    @Test
    public void sholdCorrectCalculateNextSegmentIndex() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.curAbsWalIdx(5);

        //when: interrupt waiting.
        long segmentIndex = aware.nextAbsoluteSegmentIndex();

        //then: IgniteInterruptedCheckedException should be throw.
        assertThat(segmentIndex, CoreMatchers.is(6L));
    }

    @Test
    public void sholdFinishWaitNextAbsoluteIndex_WhenMarkAsArchivedFirst() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(2);
        aware.curAbsWalIdx(1);
        aware.setLastArchivedAbsoluteIndex(-1);

        IgniteInternalFuture future = awaitThread(aware::nextAbsoluteSegmentIndex);

        //when: interrupt waiting.
        aware.markAsMovedToArchive(0);

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdFinishWaitNextAbsoluteIndex_WhenSetToArchivedFirst() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(2);
        aware.curAbsWalIdx(1);
        aware.setLastArchivedAbsoluteIndex(-1);

        IgniteInternalFuture future = awaitThread(aware::nextAbsoluteSegmentIndex);

        //when: interrupt waiting.
        aware.setLastArchivedAbsoluteIndex(0);

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdFinishWaitNextAbsoluteIndex_WhenOnlyForceInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(2);
        aware.curAbsWalIdx(2);
        aware.setLastArchivedAbsoluteIndex(-1);

        IgniteInternalFuture future = awaitThread(aware::nextAbsoluteSegmentIndex);

        //when: interrupt waiting.
        aware.interrupt();

        assertFutureIsNotFinish(future);

        //when: force interrupt waiting.
        aware.forceInterrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertTrue(future.get(10) instanceof IgniteInterruptedCheckedException);
    }

    @Test
    public void sholdFinishSegmentArchived_WhenSetExactWatingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: interrupt waiting.
        aware.setLastArchivedAbsoluteIndex(5);

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdFinishSegmentArchived_WhenMarkExactWatingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: interrupt waiting.
        aware.markAsMovedToArchive(5);

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdFinishSegmentArchived_WhenSetGreaterThanWatingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: interrupt waiting.
        aware.setLastArchivedAbsoluteIndex(7);

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdFinishSegmentArchived_WhenMarkGreaterThanWatingSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: interrupt waiting.
        aware.markAsMovedToArchive(7);

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdFinishSegmentArchived_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.curAbsWalIdx(5);
        aware.setLastArchivedAbsoluteIndex(4);

        IgniteInternalFuture future = awaitThread(() -> aware.awaitSegmentArchived(5));

        //when: interrupt waiting.
        aware.interrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertTrue(future.get(10) instanceof IgniteInterruptedCheckedException);
    }

    @Test
    public void sholdMarkAsMovedToArchive_WhenReleaseLockedSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.checkCanReadArchiveOrReserveWorkSegment(5);

        IgniteInternalFuture future = awaitThread(() -> aware.markAsMovedToArchive(5));

        //when: interrupt waiting.
        aware.releaseWorkSegment(5);

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdMarkAsMovedToArchive_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.checkCanReadArchiveOrReserveWorkSegment(5);

        IgniteInternalFuture future = awaitThread(() -> aware.markAsMovedToArchive(5));

        //when: interrupt waiting.
        aware.interrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertFalse(future.get(10) instanceof IgniteInterruptedCheckedException);
        assertEquals(5, aware.lastArchivedAbsoluteIndex());
    }

    @Test
    public void sholdFinishWaitSegmentToCompress_WhenSetLastArchivedSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.lastCompressedIdx(5);
        aware.allowCompressionUntil(7);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentToCompress);

        //when: interrupt waiting.
        aware.setLastArchivedAbsoluteIndex(6);

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdFinishWaitSegmentToCompress_WhenMarkLastArchivedSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.lastCompressedIdx(5);
        aware.allowCompressionUntil(7);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentToCompress);

        //when: interrupt waiting.
        aware.markAsMovedToArchive(6);

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdFinishWaitSegmentToCompress_WhenSetAllowCompressSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.lastCompressedIdx(5);
        aware.setLastArchivedAbsoluteIndex(6);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentToCompress);

        //when: interrupt waiting.
        aware.allowCompressionUntil(7);

        //then: IgniteInterruptedCheckedException should be throw.
        future.get(10);
    }

    @Test
    public void sholdCorrectCalculateNextCompressSegment() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.lastCompressedIdx(5);
        aware.setLastArchivedAbsoluteIndex(6);
        aware.allowCompressionUntil(7);
        aware.lastTruncatedArchiveIdx(7);

        long segmentToCompress = aware.waitNextSegmentToCompress();

        //when: interrupt waiting.
        assertEquals(8, segmentToCompress);
    }

    @Test
    public void sholdFinishWaitSegmentToCompress_WhenInterruptWasCall() throws IgniteCheckedException, InterruptedException {
        //given: thread which awaited segment.
        SegmentAware aware = new SegmentAware(10);
        aware.lastCompressedIdx(5);

        IgniteInternalFuture future = awaitThread(aware::waitNextSegmentToCompress);

        //when: interrupt waiting.
        aware.interrupt();

        //then: IgniteInterruptedCheckedException should be throw.
        assertTrue(future.get(10) instanceof IgniteInterruptedCheckedException);
    }

    private void assertFutureIsNotFinish(IgniteInternalFuture future) throws IgniteCheckedException {
        try {
            future.get(10);

            fail("Timeout should be appear because thread should be still work");
        }
        catch (IgniteFutureTimeoutCheckedException ignore) {

        }
    }

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

    interface Waiter {
        void await() throws IgniteInterruptedCheckedException;
    }
}