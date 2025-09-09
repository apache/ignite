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

/**
 * Represents speed calculation of some progress starting from zero.
 * This class not only takes into account current progress value, but also 3 previous values, each of them
 * is pushed to history when {@link #closeInterval()} is called.
 */
class ProgressSpeedCalculation {
    /**
     * Measurement used to calculate average speed. History recording is disabled.
     */
    private final IntervalBasedMeasurement measurement = new IntervalBasedMeasurement();

    /**
     * Sets the value which was reached by the progress.
     *
     * @param progress  progress value
     * @param nanoTime  time instant
     */
    public void setProgress(long progress, long nanoTime) {
        measurement.setCounter(progress, nanoTime);
    }

    /**
     * Returns speed of progress in operations per second calculated from the current value and 3 latest historical
     * intervals. This method may change internal state (namely, initialize the current interval).
     *
     * @param nanoTime time instant at which the speed is to be calculated
     * @return average ops per second
     */
    public long getOpsPerSecond(long nanoTime) {
        return measurement.getSpeedOpsPerSec(nanoTime);
    }

    /**
     * Returns speed of progress in operations per second calculated from the current value and 3 latest historical
     * intervals. This method does not change internal state.
     *
     * @return ops per second
     */
    public long getOpsPerSecondReadOnly() {
        return measurement.getSpeedOpsPerSecReadOnly();
    }

    /**
     * Closes the current interval by pushing it to the history. The 3 latest intervals put into history affect
     * the average speed calculation via {@link #getOpsPerSecond(long)} and {@link #getOpsPerSecondReadOnly()}.
     */
    public void closeInterval() {
        measurement.finishInterval();
    }
}
