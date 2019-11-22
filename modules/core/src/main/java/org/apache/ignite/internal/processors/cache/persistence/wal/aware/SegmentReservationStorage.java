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

import java.util.Arrays;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;

/**
 * Segment reservations storage: Protects WAL segments from deletion during WAL log cleanup.
 */
class SegmentReservationStorage {
    /** Logger. */
    protected IgniteLogger log;

    public SegmentReservationStorage(IgniteLogger log) {
        this.log = log;
    }

    /**
     * Maps absolute segment index to reservation counter. If counter > 0 then we wouldn't delete all segments which has
     * index >= reserved segment index. Guarded by {@code this}.
     */
    private NavigableMap<Long, Integer> reserved = new TreeMap<>();

    /**
     * @param absIdx Index for reservation.
     */
    void reserve(long absIdx) {
        synchronized(this) {
            reserved.merge(absIdx, 1, (a, b) -> a + b);
        }

        log.info("RESERVE :: " + absIdx + " :: " + reserved.toString() + "\n" + threadDump());
    }

    private String threadDump() {
        return Arrays.stream(Thread.currentThread().getStackTrace()).map(StackTraceElement::toString).collect(Collectors.joining("\n"));
    }

    /**
     * Checks if segment is currently reserved (protected from deletion during WAL cleanup).
     *
     * @param absIdx Index for check reservation.
     * @return {@code True} if index is reserved.
     */
    boolean reserved(long absIdx) {
        boolean reserved;
        synchronized(this) {
            reserved = this.reserved.floorKey(absIdx) != null;
        }

        log.info("RESERVE :: " + absIdx + " :: " + reserved + "\n" + threadDump());

        return reserved;
    }

    /**
     * @param absIdx Reserved index.
     */
    void release(long absIdx) {
        synchronized(this) {
            Integer cur = reserved.get(absIdx);

            assert cur != null && cur >= 1 : "cur=" + cur + ", absIdx=" + absIdx + ", reserved=" + reserved.toString();

            if (cur == 1)
                reserved.remove(absIdx);
            else
                reserved.put(absIdx, cur - 1);
        }

        log.info("RESERVE :: " + absIdx + " :: " + reserved.toString() + "\n" + threadDump());
    }
}
