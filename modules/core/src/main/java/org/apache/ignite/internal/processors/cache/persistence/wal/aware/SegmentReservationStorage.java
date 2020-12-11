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

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Segment reservations storage: Protects WAL segments from deletion during WAL log cleanup.
 */
class SegmentReservationStorage {
    /**
     * Maps absolute segment index to reservation counter. If counter > 0 then we wouldn't delete all segments which has
     * index >= reserved segment index. Guarded by {@code this}.
     */
    private final NavigableMap<Long, Integer> reserved = new TreeMap<>();

    /** Maximum segment index that can be reserved. */
    private long minReserveIdx = -1;

    /**
     * Segment reservation. It will be successful if segment is {@code >} than the {@link #minReserveIdx minimum}.
     *
     * @param absIdx Index for reservation.
     * @return {@code True} if the reservation was successful.
     */
    synchronized boolean reserve(long absIdx) {
        if (absIdx > minReserveIdx) {
            reserved.merge(absIdx, 1, Integer::sum);

            return true;
        }

        return false;
    }

    /**
     * Checks if segment is currently reserved (protected from deletion during WAL cleanup).
     *
     * @param absIdx Index for check reservation.
     * @return {@code True} if index is reserved.
     */
    synchronized boolean reserved(long absIdx) {
        return reserved.floorKey(absIdx) != null;
    }

    /**
     * @param absIdx Reserved index.
     */
    synchronized void release(long absIdx) {
        Integer cur = reserved.get(absIdx);

        assert cur != null && cur >= 1 : "cur=" + cur + ", absIdx=" + absIdx;

        if (cur == 1)
            reserved.remove(absIdx);
        else
            reserved.put(absIdx, cur - 1);
    }

    /**
     * Increasing minimum segment index that can be reserved.
     * Value will be updated if it is greater than the current one.
     * If segment is already reserved, the update will fail.
     *
     * @param absIdx Absolut segment index.
     * @return {@code True} if update is successful.
     */
    synchronized boolean minReserveIndex(long absIdx) {
        if (reserved(absIdx))
            return false;

        minReserveIdx = Math.max(minReserveIdx, absIdx);

        return true;
    }
}
