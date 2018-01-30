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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class IgniteThrottlingUnitTest {
    /** Page memory 2 g. */
    private PageMemoryImpl pageMemory2g = Mockito.mock(PageMemoryImpl.class);

    {
        Mockito.when(pageMemory2g.totalPages()).thenReturn((2L * 1024 * 1024 * 1024) / 4096);
    }

    /**
     *
     */
    @Test
    public void breakInCaseTooFast() {

        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null);
        long time = throttle.getParkTime(0.67,
            (362584 + 67064) / 2,
            328787,
            1,
            60184,
            23103);

        assertTrue(time > 0);
    }

    /**
     *
     */
    @Test
    public void noBreakIfNotFastWrite() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null);

        long time = throttle.getParkTime(0.47,
            ((362584 + 67064) / 2),
            328787,
            1,
            20103,
            23103);

        assertTrue(time == 0);
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

        Thread.sleep(200);

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
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null);

        assertTrue(throttle.getParkTime(0.01, 100,400000,
            1,
            20103,
            23103) == 0);

        //mark speed 22413 for mark all remaining as dirty
        long time = throttle.getParkTime(0.024, 100, 400000,
            1,
            24000,
            23103);
        assertTrue(time > 0);

        assertTrue(throttle.getParkTime(0.01,
            100,
            400000,
            1,
            22412,
            23103) == 0);
    }

    /**
     *
     */
    @Test
    public void enforceThrottleAtTheEndOfCp() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null);
        long time1 = throttle.getParkTime(0.70, 300000, 400000,
            1, 20200, 23000);
        long time2 = throttle.getParkTime(0.71, 300000, 400000,
            1, 20200, 23000);

        assertTrue(time2 >= time1 * 2); // extra slowdown should be applied.

        long time3 = throttle.getParkTime(0.73, 300000, 400000,
            1, 20200, 23000);
        long time4 = throttle.getParkTime(0.74, 300000, 400000,
            1, 20200, 23000);

        assertTrue(time3 > time2);
        assertTrue(time4 > time3);
    }

    @Test
    public void tooMuchPagesMarkedDirty() {
        PagesWriteSpeedBasedThrottle throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null);

       // 363308	350004	348976	10604
        long time = throttle.getParkTime(0.75,
            ((350004 + 348976) / 2),
            350004-10604,
            4,
            279,
            23933);

        System.err.println(time);

        assertTrue(time == 0);
    }
}
