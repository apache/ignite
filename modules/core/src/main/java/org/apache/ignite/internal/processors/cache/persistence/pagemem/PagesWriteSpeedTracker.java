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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;

/**
 * Speed tracker for determine speed of processing based on increments or exact counters value. Measurement is perfromed
 * using several intervals. Too old measurements are dropped. To determine speed current measurement is reduced with all
 * historical.
 */
public class PagesWriteSpeedTracker {
    /** Millis in second. */
    private static final int MILLIS_IN_SECOND = 1000;

    /** Current Measurement interval atomic reference. */
    private AtomicReference<MeasurementInterval> measurementIntervalAtomicRef = new AtomicReference<>();

    /** Interval automatic switch milliseconds. Negative value means no automatic switch. */
    private final int intervalSwitchMs;

    /** Max historical measurements to keep. */
    private final int maxMeasurements;

    /** Previous (historical) measurements. */
    private final ConcurrentLinkedQueue<MeasurementInterval> prevMeasurements = new ConcurrentLinkedQueue<>();

     /**
     * Default constructor. No automatic switch, 3 historical measurements.
     */
    public PagesWriteSpeedTracker() {
        this(-1, 3);
    }

    /**
     * @param intervalSwitchMs Interval switch milliseconds.
     * @param maxMeasurements Max historical measurements to keep.
     */
    public PagesWriteSpeedTracker(int intervalSwitchMs, int maxMeasurements) {
        this.intervalSwitchMs = intervalSwitchMs;
        this.maxMeasurements = maxMeasurements;
    }

    /**
     * Gets speed, start interval (if not started).
     *
     * @param curMs current time millis.
     * @return speed in pages per second based on current data.
     */
    public long getSpeedPagesPerSec(long curMs) {
        MeasurementInterval interval = interval(curMs);

        return combinePrevAndCurrent(curMs, interval);
    }

    /**
     * Gets current speed, does not start measurement.
     *
     * @return speed in pages per second based on current data.
     */
    public long getSpeedPagesPerSecOptional() {
        MeasurementInterval interval = measurementIntervalAtomicRef.get();

        if (interval == null)
            return getHistoricalSpeed();

        return combinePrevAndCurrent(System.currentTimeMillis(), interval);
    }

    /**
     * @param curMs current time millis.
     * @param interval current measurement.
     * @return reduced with historical.
     */
    private long combinePrevAndCurrent(long curMs, @NotNull MeasurementInterval interval) {
        //measurement was created during current call, use only historical.
        if (curMs <= interval.tsStart)
            return getHistoricalSpeed();

        long msPassed = (curMs - interval.tsStart);
        long pagesProcessed = interval.cntr.get();

        return sumMeasurementWithHistorical(msPassed, pagesProcessed);
    }

    /**
     * Reduce measurements.
     *
     * @param msPassed initial milliseconds passed (probably from current measurement).
     * @param pagesProcessed initial pages processed (probably from current measurement).
     * @return speed in page per second.
     */
    private long sumMeasurementWithHistorical(long msPassed, long pagesProcessed) {
        for (MeasurementInterval prevMeasurement : prevMeasurements) {
            msPassed += (prevMeasurement.tsEnd - prevMeasurement.tsStart);
            pagesProcessed += prevMeasurement.cntr.get();
        }

        return msPassed == 0 ? 0 : pagesProcessed * MILLIS_IN_SECOND / msPassed;
    }

    /**
     * @return speed in page per second from historical only measurements.
     */
    private long getHistoricalSpeed() {
        return sumMeasurementWithHistorical(0, 0);
    }

    /**
     * Gets or creates measurement interval, performs switch to new measurement by timeout.
     * @param curMs current time.
     * @return interval to use.
     */
    @NotNull private MeasurementInterval interval(long curMs) {
        MeasurementInterval interval;

        do {
            interval = measurementIntervalAtomicRef.get();
            if (interval == null) {
                MeasurementInterval newInterval = new MeasurementInterval(curMs);

                if (measurementIntervalAtomicRef.compareAndSet(null, newInterval))
                    interval = newInterval;
                else
                    continue;
            }

            if (intervalSwitchMs > 0 && (curMs - interval.tsStart) > intervalSwitchMs) {
                MeasurementInterval newInterval = new MeasurementInterval(curMs);

                if (measurementIntervalAtomicRef.compareAndSet(interval, newInterval)) {
                    interval.tsEnd = curMs;

                    pushToHistory(interval);
                }
            }
        }
        while (interval == null);

        return interval;
    }

    /**
     * @param interval finished interval to push to history.
     */
    private void pushToHistory(MeasurementInterval interval) {
        prevMeasurements.offer(interval);

        if (prevMeasurements.size() > maxMeasurements)
            prevMeasurements.remove();
    }

    /**
     * Increments page count in current interval, switches interval if needed.
     */
    public void incrementCounter() {
        interval(System.currentTimeMillis()).cntr.incrementAndGet();
    }

    /**
     * @param val new value to set.
     * @param curMs current time.
     */
    public void setCounter(long val, long curMs) {
        interval(curMs).cntr.set(val);
    }

    /**
     * Manually switch interval to empty (not started measurement).
     */
    public void finishInterval() {
        while (true) {
            MeasurementInterval interval = measurementIntervalAtomicRef.get();

            if (interval == null)
                return;

            if (measurementIntervalAtomicRef.compareAndSet(interval, null)) {
                interval.tsEnd = System.currentTimeMillis();

                pushToHistory(interval);

                return;
            }
        }
    }

    /**
     * Measurement interval, completed or open.
     */
    private static class MeasurementInterval {
        /** Counter of performed operations, pages. */
        private AtomicLong cntr = new AtomicLong();
        /** Timestamp of measurement start. */
        private final long tsStart;

        /** Timestamp  of measurement end. 0 for open measurements */
        private volatile long tsEnd;

        /**
         * @param tsStart Timestamp of measurement start.
         */
        public MeasurementInterval(long tsStart) {
            this.tsStart = tsStart;
        }
    }
}
