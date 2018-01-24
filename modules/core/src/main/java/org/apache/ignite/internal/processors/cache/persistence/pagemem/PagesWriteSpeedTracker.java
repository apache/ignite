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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;

public class PagesWriteSpeedTracker {

    private static final int MILLIS_IN_SECOND = 1000;
    private AtomicReference<MeasurementInterval> measurementIntervalAtomicReference = new AtomicReference<>();
    private final int intervalSwitchMs;
    private volatile MeasurementInterval prevMeasurement;

    public PagesWriteSpeedTracker() {
        this(-1);
    }

    public PagesWriteSpeedTracker(int intervalSwitchMs) {
        this.intervalSwitchMs = intervalSwitchMs;
    }

    public long getSpeedPagesPerSec(long curMs) {
        MeasurementInterval interval = interval(curMs);

        return combinePrevAndCurrent(curMs, interval);
    }

    public long getSpeedPagesPerSecOptional() {
        MeasurementInterval interval = measurementIntervalAtomicReference.get();

        if (interval == null)
            return getHistoricalSpeed();

        return combinePrevAndCurrent(System.currentTimeMillis(), interval);
    }

    private long combinePrevAndCurrent(long curMs, MeasurementInterval interval) {
        if (prevMeasurement == null)
            return interval.getSpeedPagesPerSec(curMs);

        if (curMs <= interval.tsStart) { //measurement is created only now
            return getHistoricalSpeed();
        }

        long msPassed = (curMs - interval.tsStart) + (prevMeasurement.tsEnd - prevMeasurement.tsStart);
        long pagesMarked = interval.counter.get() + prevMeasurement.counter.get();

        return msPassed == 0 ? 0 : pagesMarked * MILLIS_IN_SECOND / msPassed;
    }

    private long getHistoricalSpeed() {
        if (prevMeasurement == null)
            return 0;
        else
            return prevMeasurement.getHistSpeedPagesPerSec();
    }

    @NotNull private MeasurementInterval interval(long curMs) {
        MeasurementInterval interval;
        do {
            interval = measurementIntervalAtomicReference.get();
            if (interval == null) {
                MeasurementInterval newInterval = new MeasurementInterval(curMs);
                if (measurementIntervalAtomicReference.compareAndSet(null, newInterval)) {
                    interval = newInterval;
                }
                else
                    continue;
            }

            if (intervalSwitchMs > 0 && (curMs - interval.tsStart) > intervalSwitchMs) {
                MeasurementInterval newInterval = new MeasurementInterval(curMs);

                if (measurementIntervalAtomicReference.compareAndSet(interval, newInterval)) {
                    interval.tsEnd = curMs;
                    prevMeasurement = interval;
                }
            }
        }
        while (interval == null);
        return interval;
    }

    public void incrementCounter() {
        interval(System.currentTimeMillis()).counter.incrementAndGet();
    }

    public void setCounter(long value) {
        interval(System.currentTimeMillis()).counter.set(value);
    }

    public void finishInterval() {
        while (true) {
            MeasurementInterval interval = measurementIntervalAtomicReference.get();

            if (interval == null)
                return;

            if (measurementIntervalAtomicReference.compareAndSet(interval, null)) {
                interval.tsEnd = System.currentTimeMillis();
                prevMeasurement = interval;
            }
        }
    }

    private static class MeasurementInterval {
        private AtomicLong counter = new AtomicLong();
        private final long tsStart;
        private volatile long tsEnd;

        public MeasurementInterval(long ms) {
            this.tsStart = ms;
        }

        private long getSpeedPagesPerSec(long curMs) {
            long msPassed = curMs - tsStart;
            long pagesMarked = counter.get();
            return msPassed == 0 ? 0 : pagesMarked * MILLIS_IN_SECOND / msPassed;
        }

        private long getHistSpeedPagesPerSec() {
            return getSpeedPagesPerSec(tsEnd);
        }
    }
}
