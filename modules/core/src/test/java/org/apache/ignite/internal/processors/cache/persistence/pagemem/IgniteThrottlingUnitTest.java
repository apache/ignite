/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgressImpl;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsProcessor;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import static java.lang.Thread.State.TIMED_WAITING;
import static org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesWriteThrottlePolicy.CP_BUF_THROTTLING_THRESHOLD_FILL_RATE;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class IgniteThrottlingUnitTest extends GridCommonAbstractTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = Timeout.millis((int)GridTestUtils.DFLT_TEST_TIMEOUT);

    /** Logger. */
    private final IgniteLogger log = new NullLogger();

    /** Page memory 2 g. */
    private final PageMemoryImpl pageMemory2g = mock(PageMemoryImpl.class);

    /** State checker. */
    private final CheckpointLockStateChecker stateChecker = () -> true;

    /** {@link CheckpointProgress} mock. */
    private final CheckpointProgress progress = mock(CheckpointProgress.class);

    /** {@link CheckpointProgress} provider that always provides the mock above. */
    private final IgniteOutClosure<CheckpointProgress> cpProvider = () -> progress;

    {
        when(pageMemory2g.totalPages()).thenReturn((2L * 1024 * 1024 * 1024) / 4096);

        IgniteConfiguration cfg = new IgniteConfiguration().setMetricExporterSpi(new NoopMetricExporterSpi());

        GridTestKernalContext ctx = new GridTestKernalContext(new GridTestLog4jLogger(), cfg);

        ctx.add(new GridMetricManager(ctx));
        ctx.add(new PerformanceStatisticsProcessor(ctx));

        DataRegionMetricsImpl metrics = new DataRegionMetricsImpl(new DataRegionConfiguration(), ctx);

        when(pageMemory2g.metrics()).thenReturn(metrics);
    }

    /**
     * Tests that the speed-based throttler throttles when writing faster than target speed, AND the dirty ratio
     * is above the target ratio.
     */
    @Test
    public void shouldThrottleWhenWritingTooFast() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null,
            stateChecker, false, log);

        long parkTime = throttle.getCleanPagesProtectionParkTime(0.67,
            (362584 + 67064) / 2,
            328787,
            1,
            60184,
            23103);

        assertTrue(parkTime > 0);
    }

    /**
     *
     */
    @Test
    public void shouldNotThrottleWhenWritingSlowly() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null,
            stateChecker, false, log);

        long parkTime = throttle.getCleanPagesProtectionParkTime(0.47,
            ((362584 + 67064) / 2),
            328787,
            1,
            20103,
            23103);

        assertEquals(0, parkTime);
    }

    /**
     * Tests that the speed-based throttler does NOT throttle when there are plenty clean pages, even if writing
     * faster than the current checkpoint speed.
     */
    @Test
    public void shouldNotThrottleWhenThereArePlentyCleanPages() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null,
            stateChecker, false, log);

        long parkTime = throttle.getCleanPagesProtectionParkTime(0.0,
            (362584 + 67064) / 2,
            328787,
            1,
            60184,
            23103);

        assertEquals(0, parkTime);
    }

    /**
     * Test that time to park is calculated according to both cpSpeed and mark dirty speed (in case if checkpoint buffer
     * is not full).
     */
    @Test
    public void testCorrectTimeToPark() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null,
            stateChecker, false, log);

        int markDirtySpeed = 34422;
        int cpWriteSpeed = 19416;
        long time = throttle.getCleanPagesProtectionParkTime(0.67,
            ((903150 + 227217) / 2),
            903150,
            1,
            markDirtySpeed,
            cpWriteSpeed);

        assertEquals(415110, time);
    }

    /**
     * @throws InterruptedException if interrupted.
     */
    @Test
    public void averageCalculation() throws InterruptedException {
        IntervalBasedMeasurement measurement = new IntervalBasedMeasurement(100, 1);

        for (int i = 0; i < 1000; i++)
            measurement.addMeasurementForAverageCalculation(100);

        assertEquals(100, measurement.getAverage());

        Thread.sleep(220);

        assertEquals(0, measurement.getAverage());

        assertEquals(0, measurement.getSpeedOpsPerSec(System.nanoTime()));
    }

    /**
     * @throws InterruptedException if interrupted.
     */
    @Test
    public void speedCalculation() throws InterruptedException {
        IntervalBasedMeasurement measurement = new IntervalBasedMeasurement(100, 1);

        for (int i = 0; i < 1000; i++)
            measurement.setCounter(i, System.nanoTime());

        long speed = measurement.getSpeedOpsPerSec(System.nanoTime());
        System.out.println("speed measured " + speed);
        assertTrue(speed > 1000);

        Thread.sleep(230);

        assertEquals(0, measurement.getSpeedOpsPerSec(System.nanoTime()));
    }

    /**
     * @throws InterruptedException if interrupted.
     */
    @Test
    public void speedWithDelayCalculation() throws InterruptedException {
        IntervalBasedMeasurement measurement = new IntervalBasedMeasurement(100, 1);

        int runs = 10;
        int nanosPark = 100;
        int multiplier = 100000;
        for (int i = 0; i < runs; i++) {
            measurement.setCounter(i * multiplier, System.nanoTime());

            LockSupport.parkNanos(nanosPark);
        }

        long speed = measurement.getSpeedOpsPerSec(System.nanoTime());

        assertTrue(speed > 0);
        long maxSpeed = (TimeUnit.SECONDS.toNanos(1) * multiplier * runs) / ((long)(runs * nanosPark));
        assertTrue(speed < maxSpeed);

        Thread.sleep(200);

        assertEquals(0, measurement.getSpeedOpsPerSec(System.nanoTime()));
    }

    /**
     *
     */
    @Test
    public void beginOfCp() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null,
            stateChecker, false, log);

        assertEquals(0, throttle.getCleanPagesProtectionParkTime(0.01, 100, 400000,
            1,
            20103,
            23103));

        //mark speed 22413 for mark all remaining as dirty
        long time = throttle.getCleanPagesProtectionParkTime(0.024, 100, 400000,
            1,
            24000,
            23103);
        assertTrue(time > 0);

        assertEquals(0, throttle.getCleanPagesProtectionParkTime(0.01,
            100,
            400000,
            1,
            22412,
            23103));
    }

    /**
     *
     */
    @Test
    public void enforceThrottleAtTheEndOfCp() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null,
            stateChecker, false, log);

        long time1 = throttle.getCleanPagesProtectionParkTime(0.70, 300000, 400000,
            1, 20200, 23000);
        long time2 = throttle.getCleanPagesProtectionParkTime(0.71, 300000, 400000,
            1, 20200, 23000);

        assertTrue(time2 >= time1 * 2); // extra slowdown should be applied.

        long time3 = throttle.getCleanPagesProtectionParkTime(0.73, 300000, 400000,
            1, 20200, 23000);
        long time4 = throttle.getCleanPagesProtectionParkTime(0.74, 300000, 400000,
            1, 20200, 23000);

        assertTrue(time3 > time2);
        assertTrue(time4 > time3);
    }

    /**
     *
     */
    @Test
    public void doNotThrottleWhenDirtyPagesRatioIsTooHigh() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null,
            stateChecker, false, log);

        // 363308 350004 348976 10604
        long time = throttle.getCleanPagesProtectionParkTime(0.75,
            ((350004 + 348976) / 2),
            350004 - 10604,
            4,
            279,
            23933);

        assertEquals(0, time);
    }

    /**
     * @throws IgniteInterruptedCheckedException if failed.
     */
    @Test
    public void wakeupSpeedBaseThrottledThreadOnCheckpointFinish() throws IgniteInterruptedCheckedException {
        //given: Enabled throttling with EXPONENTIAL level.
        CheckpointProgressImpl cl0 = mock(CheckpointProgressImpl.class);
        when(cl0.writtenPagesCounter()).thenReturn(new AtomicInteger(200));

        IgniteOutClosure<CheckpointProgress> cpProgress = mock(IgniteOutClosure.class);
        when(cpProgress.apply()).thenReturn(cl0);

        PagesWriteThrottlePolicy plc = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProgress, stateChecker,
            false, log) {
            @Override protected void doPark(long throttleParkTimeNs) {
                //Force parking to long time.
                super.doPark(TimeUnit.SECONDS.toNanos(1));
            }
        };

        simulateCheckpointBufferInDangerZoneSituation();

        AtomicBoolean stopLoad = new AtomicBoolean();
        List<Thread> loadThreads = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            loadThreads.add(new Thread(
                () -> {
                    while (!stopLoad.get())
                        plc.onMarkDirty(true);
                },
                "load-" + i
            ));
        }

        try {
            loadThreads.forEach(Thread::start);

            //and: All load threads are parked.
            for (Thread t : loadThreads)
                assertTrue(t.getName(), waitForCondition(() -> t.getState() == TIMED_WAITING, 1000L));

            //when: Disable throttling
            simulateCheckpointBufferInSafeZoneSituation();
            stopReportingCheckpointProgress(cpProgress);

            //and: Finish the checkpoint.
            plc.onFinishCheckpoint();

            //then: All load threads should be unparked.
            for (Thread t : loadThreads)
                assertTrue(t.getName(), waitForCondition(() -> t.getState() != TIMED_WAITING, 500L));

            for (Thread t : loadThreads)
                assertNotEquals(t.getName(), TIMED_WAITING, t.getState());
        }
        finally {
            stopLoad.set(true);
        }
    }

    /***/
    private void stopReportingCheckpointProgress(IgniteOutClosure<CheckpointProgress> cpProgress) {
        when(cpProgress.apply()).thenReturn(null);
    }

    /**
     *
     */
    @Test
    public void wakeupThrottledThread() throws IgniteInterruptedCheckedException {
        when(pageMemory2g.checkpointBufferPagesSize()).thenReturn(100);

        PagesWriteThrottlePolicy plc = new PagesWriteThrottle(pageMemory2g, null, stateChecker,
            true, false, log);

        AtomicBoolean stopLoad = new AtomicBoolean();
        List<Thread> loadThreads = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            loadThreads.add(new Thread(
                () -> {
                    while (!stopLoad.get())
                        plc.onMarkDirty(true);
                },
                "load-" + i
            ));
        }

        AtomicInteger checkpointBufPagesCnt = new AtomicInteger(70);

        when(pageMemory2g.checkpointBufferPagesCount()).thenAnswer(mock -> checkpointBufPagesCnt.get());

        try {
            loadThreads.forEach(Thread::start);

            for (int i = 0; i < 1_000; i++)
                loadThreads.forEach(LockSupport::unpark);

            // Awaiting that all load threads are parked.
            for (Thread t : loadThreads)
                assertTrue(t.getName(), waitForCondition(() -> t.getState() == TIMED_WAITING, 500L));

            // Disable throttling
            checkpointBufPagesCnt.set(50);

            // Awaiting that all load threads are unparked.
            for (Thread t : loadThreads)
                assertTrue(t.getName(), waitForCondition(() -> t.getState() != TIMED_WAITING, 500L));

            for (Thread t : loadThreads)
                assertNotEquals(t.getName(), TIMED_WAITING, t.getState());
        }
        finally {
            stopLoad.set(true);
        }
    }

    /**
     *
     */
    @Test
    public void warningInCaseTooMuchThrottling() {
        AtomicInteger warnings = new AtomicInteger(0);
        IgniteLogger log = mock(IgniteLogger.class);

        when(log.isInfoEnabled()).thenReturn(true);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();

            System.out.println("log.info() called with arguments: " + Arrays.toString(args));

            warnings.incrementAndGet();

            return null;
        }).when(log).info(anyString());

        AtomicInteger written = new AtomicInteger();

        Mockito.when(progress.writtenPagesCounter()).thenReturn(written);

        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider,
            stateChecker, false, log) {
            @Override protected void doPark(long throttleParkTimeNs) {
                //do nothing
            }
        };
        throttle.onBeginCheckpoint();
        written.set(200); //emulating some pages written

        for (int i = 0; i < 100000; i++) {
            //emulating high load on marking
            throttle.onMarkDirty(false);

            if (throttle.throttleWeight() > PagesWriteSpeedBasedThrottle.WARN_THRESHOLD)
                break;
        }

        for (int i = 0; i < 1000; i++) {
            //emulating additional page writes to be sure log message is generated

            throttle.onMarkDirty(false);

            if (warnings.get() > 0)
                break;
        }

        assertTrue(warnings.get() > 0);
    }

    /***/
    @Test
    public void speedBasedThrottleShouldThrottleWhenCheckpointBufferIsInDangerZone() {
        simulateCheckpointProgressIsStarted();
        simulateCheckpointBufferInDangerZoneSituation();
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider,
            stateChecker, false, log);

        throttle.onMarkDirty(true);

        assertThatThrottlingHappened(throttle);
    }

    /***/
    private void assertThatThrottlingHappened(PagesWriteSpeedBasedThrottle throttle) {
        assertThat(throttle.throttleParkTime(), greaterThan(0L));
    }

    /***/
    private void simulateCheckpointBufferInDangerZoneSituation() {
        when(pageMemory2g.checkpointBufferPagesSize()).thenReturn(100);
        when(pageMemory2g.checkpointBufferPagesCount()).thenReturn(100);
    }

    /***/
    @Test
    public void speedBasedThrottleShouldThrottleWhenCheckpointCountersAreNotReadyYetButCheckpointBufferIsInDangerZone() {
        simulateCheckpointProgressNotYetStarted();
        simulateCheckpointBufferInDangerZoneSituation();
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider,
            stateChecker, false, log);

        throttle.onMarkDirty(true);

        assertThatThrottlingHappened(throttle);
    }

    /***/
    private void simulateCheckpointProgressNotYetStarted() {
        when(progress.writtenPagesCounter()).thenReturn(null);
    }

    /***/
    @Test
    public void speedBasedThrottleShouldNotLeaveTracesInStatisticsWhenCPBufferIsInSafeZoneAndProgressIsNotYetStarted() {
        simulateCheckpointProgressNotYetStarted();
        simulateCheckpointBufferInSafeZoneSituation();
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider,
            stateChecker, false, log);

        throttle.onMarkDirty(true);

        assertThat(throttle.throttleParkTime(), is(0L));
    }

    /***/
    private void simulateCheckpointBufferInSafeZoneSituation() {
        when(pageMemory2g.checkpointBufferPagesSize()).thenReturn(100);
        when(pageMemory2g.checkpointBufferPagesCount()).thenReturn(0);
    }

    /***/
    @Test
    public void speedBasedThrottleShouldResetCPBufferProtectionParkTimeWhenItSeesThatCPBufferIsInSafeZoneAndThePageIsInCheckpoint() {
        // preparations
        simulateCheckpointProgressIsStarted();
        AtomicLong parkTimeNanos = new AtomicLong();
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider,
            stateChecker, false, log) {
            @Override protected void doPark(long throttleParkTimeNs) {
                super.doPark(1);
                parkTimeNanos.set(throttleParkTimeNs);
            }
        };

        // test script starts

        // cause a couple of CP Buffer protection parks
        simulateCheckpointBufferInDangerZoneSituation();
        throttle.onMarkDirty(true);
        throttle.onMarkDirty(true);
        assertThat(parkTimeNanos.get(), is(4200L));

        // cause the counter to be reset
        simulateCheckpointBufferInSafeZoneSituation();
        throttle.onMarkDirty(true);

        // check that next CP Buffer protection park starts from the beginning
        simulateCheckpointBufferInDangerZoneSituation();
        throttle.onMarkDirty(true);
        assertThat(parkTimeNanos.get(), is(4000L));
    }

    /***/
    private void simulateCheckpointProgressIsStarted() {
        when(progress.writtenPagesCounter()).thenReturn(new AtomicInteger(1000));
    }

    /***/
    @Test
    public void speedBasedThrottleShouldNotResetCPBufferProtectionParkTimeWhenItSeesThatCPBufferIsInSafeZoneAndThePageIsNotInCheckpoint() {
        // preparations
        simulateCheckpointProgressIsStarted();
        AtomicLong parkTimeNanos = new AtomicLong();
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider,
            stateChecker, false, log) {
            @Override protected void doPark(long throttleParkTimeNs) {
                super.doPark(1);
                parkTimeNanos.set(throttleParkTimeNs);
            }
        };

        // test script starts

        // cause a couple of CP Buffer protection parks
        simulateCheckpointBufferInDangerZoneSituation();
        throttle.onMarkDirty(true);
        throttle.onMarkDirty(true);
        assertThat(parkTimeNanos.get(), is(4200L));

        // this should NOT cause the counter to be reset
        simulateCheckpointBufferInSafeZoneSituation();
        throttle.onMarkDirty(false);

        // check that next CP Buffer protection park is the third member of the progression
        simulateCheckpointBufferInDangerZoneSituation();
        throttle.onMarkDirty(true);
        assertThat(parkTimeNanos.get(), is(4410L));
    }

    /***/
    @Test
    public void speedBasedThrottleShouldReportCpWriteSpeedWhenThePageIsNotInCheckpointAndProgressIsReported()
            throws InterruptedException {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider,
            stateChecker, false, log);
        simulateCheckpointProgressIsStarted();
        allowSomeTimeToPass();
        throttle.onMarkDirty(false);

        assertThat(throttle.getCpWriteSpeed(), is(greaterThan(0L)));
    }

    /***/
    private void allowSomeTimeToPass() throws InterruptedException {
        Thread.sleep(10);
    }

    /***/
    @Test
    public void speedBasedThrottleShouldResetCPProgressToZeroOnCheckpointStart() throws InterruptedException {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider,
                stateChecker, false, log);
        simulateCheckpointProgressIsStarted();
        allowSomeTimeToPass();
        throttle.onMarkDirty(false);

        throttle.onBeginCheckpoint();

        // verify progress speed to make a conclusion about progress itself
        assertThat(throttle.getCpWriteSpeed(), is(0L));
    }

    /** */
    private void simulateCheckpointProgress(
        double cpBufFillRate,
        double cpRecoveryDataWriteProgress,
        double cpPageWriteProgress
    ) {
        int cpBufPagesCnt = (int)(pageMemory2g.checkpointBufferPagesSize() * cpBufFillRate);
        when(pageMemory2g.checkpointBufferPagesCount()).thenReturn(cpBufPagesCnt);

        int cpWrittenRecoveryPagesCnt = (int)(progress.currentCheckpointPagesCount() * cpRecoveryDataWriteProgress);
        when(progress.writtenRecoveryPagesCounter()).thenReturn(new AtomicInteger(cpWrittenRecoveryPagesCnt));

        int cpWrittenDataPagesCnt = (int)(progress.currentCheckpointPagesCount() * cpPageWriteProgress);
        when(progress.writtenPagesCounter()).thenReturn(new AtomicInteger(cpWrittenDataPagesCnt));
    }

    /** */
    @Test
    public void testFillRateBasedCheckpointBufferProtection() {
        int cpBufSize = 1_000;
        int cpPagesCnt = 1_000_000;

        when(progress.currentCheckpointPagesCount()).thenReturn(cpPagesCnt);
        when(pageMemory2g.checkpointBufferPagesSize()).thenReturn(cpBufSize);

        AtomicLong parkTimeNanos = new AtomicLong();
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider,
            stateChecker, true, log) {
            @Override protected void doPark(long throttleParkTimeNs) {
                super.doPark(1);
                parkTimeNanos.set(throttleParkTimeNs);
            }
        };

        parkTimeNanos.set(0);
        simulateCheckpointProgress(CP_BUF_THROTTLING_THRESHOLD_FILL_RATE - 0.01, 0.01, 0);
        throttle.onMarkDirty(true);
        assertEquals(0, parkTimeNanos.get());

        parkTimeNanos.set(0);
        simulateCheckpointProgress(CP_BUF_THROTTLING_THRESHOLD_FILL_RATE + 0.01, 0.01, 0);
        throttle.onMarkDirty(true);
        long parkTime0 = parkTimeNanos.get();
        assertTrue(parkTime0 > 0);

        parkTimeNanos.set(0);
        simulateCheckpointProgress(CP_BUF_THROTTLING_THRESHOLD_FILL_RATE + 0.1, 0.01, 0);
        throttle.onMarkDirty(true);
        long parkTime1 = parkTimeNanos.get();
        assertTrue(parkTime1 > parkTime0);

        parkTimeNanos.set(0);
        simulateCheckpointProgress(0.99, 0.5, 0);
        throttle.onMarkDirty(true);
        long parkTime2 = parkTimeNanos.get();
        assertTrue(parkTime2 > parkTime1);

        // Checkpoint progress ahead of checkpoint buffer fill rate.
        parkTimeNanos.set(0);
        simulateCheckpointProgress(0.9, 1, 0.9);
        throttle.onMarkDirty(true);
        assertEquals(0, parkTimeNanos.get());
    }
}
