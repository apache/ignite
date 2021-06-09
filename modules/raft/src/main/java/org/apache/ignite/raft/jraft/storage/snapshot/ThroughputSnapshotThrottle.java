/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage.snapshot;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.util.Utils;

/**
 * SnapshotThrottle with throughput threshold used in installSnapshot.
 */
public class ThroughputSnapshotThrottle implements SnapshotThrottle {

    private final long throttleThroughputBytes;
    private final long checkCycleSecs;
    private long lastThroughputCheckTimeUs;
    private long currThroughputBytes;
    private final Lock lock = new ReentrantLock();
    private final long baseAligningTimeUs;

    public ThroughputSnapshotThrottle(final long throttleThroughputBytes, final long checkCycleSecs) {
        this.throttleThroughputBytes = throttleThroughputBytes;
        this.checkCycleSecs = checkCycleSecs;
        this.currThroughputBytes = 0L;
        this.baseAligningTimeUs = 1000 * 1000 / checkCycleSecs;
        this.lastThroughputCheckTimeUs = this.calculateCheckTimeUs(Utils.monotonicUs());
    }

    private long calculateCheckTimeUs(final long currTimeUs) {
        return currTimeUs / this.baseAligningTimeUs * this.baseAligningTimeUs;
    }

    @Override
    public long throttledByThroughput(final long bytes) {
        long availableSize;
        final long nowUs = Utils.monotonicUs();
        final long limitPerCycle = this.throttleThroughputBytes / this.checkCycleSecs;
        this.lock.lock();
        try {
            if (this.currThroughputBytes + bytes > limitPerCycle) {
                // reading another |bytes| exceeds the limit
                if (nowUs - this.lastThroughputCheckTimeUs <= 1000 * 1000 / this.checkCycleSecs) {
                    // if time interval is less than or equal to a cycle, read more data
                    // to make full use of the throughput of current cycle.
                    availableSize = limitPerCycle - this.currThroughputBytes;
                    this.currThroughputBytes = limitPerCycle;
                }
                else {
                    // otherwise, read the data in the next cycle.
                    availableSize = bytes > limitPerCycle ? limitPerCycle : bytes;
                    this.currThroughputBytes = availableSize;
                    this.lastThroughputCheckTimeUs = calculateCheckTimeUs(nowUs);
                }
            }
            else {
                // reading another |bytes| doesn't exceed limit(less than or equal to),
                // put it in current cycle
                availableSize = bytes;
                this.currThroughputBytes += availableSize;
            }
        }
        finally {
            this.lock.unlock();
        }
        return availableSize;
    }
}
