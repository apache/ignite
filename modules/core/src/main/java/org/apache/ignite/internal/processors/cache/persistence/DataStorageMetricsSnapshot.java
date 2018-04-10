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
package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class DataStorageMetricsSnapshot implements DataStorageMetrics {
    /** */
    private float walLoggingRate;

    /** */
    private float walWritingRate;

    /** */
    private int walArchiveSegments;

    /** */
    private float walFsyncTimeAvg;

    /** */
    private long walBuffPollSpinsNum;

    /** */
    private long lastCpDuration;

    /** */
    private long lastCpLockWaitDuration;

    /** */
    private long lastCpMmarkDuration;

    /** */
    private long lastCpPagesWriteDuration;

    /** */
    private long lastCpFsyncDuration;

    /** */
    private long lastCpTotalPages;

    /** */
    private long lastCpDataPages;

    /** */
    private long lastCpCowPages;

    /**
     * @param metrics Metrics.
     */
    public DataStorageMetricsSnapshot(DataStorageMetrics metrics) {
        walLoggingRate = metrics.getWalLoggingRate();
        walWritingRate = metrics.getWalWritingRate();
        walArchiveSegments = metrics.getWalArchiveSegments();
        walFsyncTimeAvg = metrics.getWalFsyncTimeAverage();
        walBuffPollSpinsNum = metrics.getWalBuffPollSpinsRate();
        lastCpDuration = metrics.getLastCheckpointDuration();
        lastCpLockWaitDuration = metrics.getLastCheckpointLockWaitDuration();
        lastCpMmarkDuration = metrics.getLastCheckpointMarkDuration();
        lastCpPagesWriteDuration = metrics.getLastCheckpointPagesWriteDuration();
        lastCpFsyncDuration = metrics.getLastCheckpointFsyncDuration();
        lastCpTotalPages = metrics.getLastCheckpointTotalPagesNumber();
        lastCpDataPages = metrics.getLastCheckpointDataPagesNumber();
        lastCpCowPages = metrics.getLastCheckpointCopiedOnWritePagesNumber();
    }

    /** {@inheritDoc} */
    @Override public float getWalLoggingRate() {
        return walLoggingRate;
    }

    /** {@inheritDoc} */
    @Override public float getWalWritingRate() {
        return walWritingRate;
    }

    /** {@inheritDoc} */
    @Override public int getWalArchiveSegments() {
        return walArchiveSegments;
    }

    /** {@inheritDoc} */
    @Override public float getWalFsyncTimeAverage() {
        return walFsyncTimeAvg;
    }

    /** {@inheritDoc} */
    @Override public long getWalBuffPollSpinsRate() {
        return walBuffPollSpinsNum;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointDuration() {
        return lastCpDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointLockWaitDuration() {
        return lastCpLockWaitDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointMarkDuration() {
        return lastCpMmarkDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointPagesWriteDuration() {
        return lastCpPagesWriteDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointFsyncDuration() {
        return lastCpFsyncDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointTotalPagesNumber() {
        return lastCpTotalPages;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointDataPagesNumber() {
        return lastCpDataPages;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointCopiedOnWritePagesNumber() {
        return lastCpCowPages;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStorageMetricsSnapshot.class, this);
    }
}
