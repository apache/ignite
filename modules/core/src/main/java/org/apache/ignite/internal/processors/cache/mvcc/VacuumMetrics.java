/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

/**
 *
 */
public class VacuumMetrics {
    /** */
    private long cleanupRowsCnt;

    /** */
    private long scannedRowsCnt;

    /** */
    private long searchNanoTime;

    /** */
    private long cleanupNanoTime;

    /**
     * @return Cleanup rows count.
     */
    public long cleanupRowsCount() {
        return cleanupRowsCnt;
    }

    /**
     * @return Scanned rows count.
     */
    public long scannedRowsCount() {
        return scannedRowsCnt;
    }

    /**
     * @return Search nano time.
     */
    public long searchNanoTime() {
        return searchNanoTime;
    }

    /**
     * @return Cleanup nano time
     */
    public long cleanupNanoTime() {
        return cleanupNanoTime;
    }


    /**
     * @param delta Delta.
     */
    public void addCleanupRowsCnt(long delta) {
        cleanupRowsCnt += delta;
    }

    /**
     * @param delta Delta.
     */
    public void addScannedRowsCount(long delta) {
        scannedRowsCnt += delta;
    }

    /**
     * @param delta Delta.
     */
    public void addSearchNanoTime(long delta) {
        searchNanoTime += delta;
    }

    /**
     * @param delta Delta.
     */
    public void addCleanupNanoTime(long delta) {
        cleanupNanoTime += delta;
    }

    /** */
    @Override public String toString() {
        return "VacuumMetrics[" +
            "cleanupRowsCnt=" + cleanupRowsCnt +
            ", scannedRowsCnt=" + scannedRowsCnt +
            ", searchNanoTime=" + Math.round((float)searchNanoTime / 1_000_000) +
            " ms, cleanupNanoTime=" + Math.round((float)cleanupNanoTime / 1_000_000) +
            " ms]";
    }
}
