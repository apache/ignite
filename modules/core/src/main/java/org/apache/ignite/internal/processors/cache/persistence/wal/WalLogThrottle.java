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
package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.IntervalBasedMeasurement;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesWriteSpeedBasedThrottle;
import org.apache.ignite.internal.util.GridConcurrentHashSet;

/**
 *
 */
public class WalLogThrottle {

    private final int maxWalSegments;
    /** Wal. */
    private FileWriteAheadLogManager wal;
    /** Data storage config. */
    private DataStorageConfiguration dsCfg;

    /** Archiver speed, bytes per second. */
    private IntervalBasedMeasurement archiverSpeed = new IntervalBasedMeasurement(1000, 5);
    /** Logger speed. */
    private IntervalBasedMeasurement logSpeed = new IntervalBasedMeasurement(250, 3);
    /** Average park time. */
    private IntervalBasedMeasurement avgParkTime = new IntervalBasedMeasurement(250, 3);
    /** Wal segment size. */
    private long walSegmentSize;

    /** Threads set. Contains identifiers of all threads which were marking pages for current checkpoint. */
    private final GridConcurrentHashSet<Long> threadIds = new GridConcurrentHashSet<>();

    /**
     * @param wal WAL manager.
     * @param dsCfg Data storage config.
     * @param archivedMonitor Archived monitor.
     */
    WalLogThrottle(FileWriteAheadLogManager wal, DataStorageConfiguration dsCfg,
        SegmentArchivedMonitor archivedMonitor) {

        this.wal = wal;
        this.dsCfg = dsCfg;

        walSegmentSize = dsCfg.getWalSegmentSize();
        maxWalSegments = dsCfg.getWalSegments();

        archivedMonitor.addListener(this::onSegmentArchived);
    }

    /**
     * @param rec record logged
     * @param currWrHandle
     */
    void onRecordLog(WALRecord rec,
        FileWriteAheadLogManager.FileWriteHandle currWrHandle) {
        if (currWrHandle == null)
            return;

        long lastArchIdx = wal.lastAbsArchivedIdx();
        if (lastArchIdx < 0)
            return;

        if (maxWalSegments <= 1 || walSegmentSize <= 0)
            return;

        int curRecordSize = rec.size();
        logSpeed.addCounter(curRecordSize);
        threadIds.add(Thread.currentThread().getId());

        long parkTimeNs = 0;

        if (isLowSpaceRemainedInWorkDir(currWrHandle, lastArchIdx)) {
            long archiverBytePerSec = archiverSpeed.getSpeedOpsPerSecReadOnly();
            long logSpeedBytesPerSec = logSpeed.getSpeedOpsPerSecReadOnly();

            if (logSpeedBytesPerSec > 0
                && archiverBytePerSec > 0
                && logSpeedBytesPerSec > archiverBytePerSec) {
                long recPerSecSpeed = archiverBytePerSec / curRecordSize;

                parkTimeNs = PagesWriteSpeedBasedThrottle.calcDelayTime(recPerSecSpeed, threadIds.size(), 2.0);
            }
        }

        if (parkTimeNs > 0)
            LockSupport.parkNanos(parkTimeNs);

        avgParkTime.addMeasurementForAverageCalculation(parkTimeNs);
    }

    /**
     * @param pointer
     * @param lastArchIdx
     * @return
     */
    private boolean isLowSpaceRemainedInWorkDir(FileWriteAheadLogManager.FileWriteHandle pointer, long lastArchIdx) {
        long curWorkIdx = pointer.idx;

        long seqInWork = curWorkIdx - lastArchIdx;
        long segRemained = maxWalSegments - seqInWork;
        long bytesInCurSegment = walSegmentSize - pointer.written;

        long maxBytesInWorkDir = maxWalSegments * walSegmentSize;
        long bytesRemained = segRemained * walSegmentSize + bytesInCurSegment;
        return bytesRemained < maxBytesInWorkDir / 4;
    }

    /**
     * @param ignored absolute WAL segment index.
     */
    private void onSegmentArchived(Long ignored) {
        archiverSpeed.addCounter(walSegmentSize);
    }

    /**
     * @return Archiver speed, bytes per second
     */
    public long archiverSpeedBytesPerSec() {
        return archiverSpeed.getSpeedOpsPerSecReadOnly();
    }

    public long averageParkTime() {
        return avgParkTime.getAverage();
    }
}
