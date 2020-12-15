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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;

/**
 * Lock on segment protects from archiving segment.
 */
public class SegmentLockStorage extends SegmentObservable {
    /**
     * Maps absolute segment index to locks counter. Lock on segment protects from archiving segment and may come from
     * {@link FileWriteAheadLogManager.RecordsIterator} during WAL replay. Map itself is guarded by <code>this</code>.
     */
    private final Map<Long, Integer> locked = new ConcurrentHashMap<>();

    /** Maximum segment index that can be locked. */
    private volatile long minLockIdx = -1;

    /**
     * Check if WAL segment locked (protected from move to archive)
     *
     * @param absIdx Index for check reservation.
     * @return {@code True} if index is locked.
     */
    boolean locked(long absIdx) {
        return locked.containsKey(absIdx);
    }

    /**
     * Segment lock. It will be successful if segment is {@code >} than the {@link #minLockIdx minimum}.
     *
     * @param absIdx Index to lock.
     * @return {@code True} if the lock was successful.
     */
    synchronized boolean lockWorkSegment(long absIdx) {
        if (absIdx > minLockIdx) {
            locked.merge(absIdx, 1, Integer::sum);

            return true;
        }

        return false;
    }

    /**
     * @param absIdx Segment absolute index.
     */
    void releaseWorkSegment(long absIdx) {
        locked.compute(absIdx, (idx, count) -> {
            assert count != null && count >= 1 : "cur=" + count + ", absIdx=" + absIdx;

            return count == 1 ? null : count - 1;
        });

        notifyObservers(absIdx);
    }

    /**
     * Increasing minimum segment index that can be locked.
     * Value will be updated if it is greater than the current one.
     * If segment is already locked, the update will fail.
     *
     * @param absIdx Absolut segment index.
     * @return {@code True} if update is successful.
     */
    synchronized boolean minLockIndex(long absIdx) {
        if (locked(absIdx))
            return false;

        minLockIdx = Math.max(minLockIdx, absIdx);

        return true;
    }
}
