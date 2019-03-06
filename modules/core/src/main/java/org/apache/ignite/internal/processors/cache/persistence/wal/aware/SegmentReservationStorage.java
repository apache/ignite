/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    private NavigableMap<Long, Integer> reserved = new TreeMap<>();

    /**
     * @param absIdx Index for reservation.
     */
    synchronized void reserve(long absIdx) {
        reserved.merge(absIdx, 1, (a, b) -> a + b);
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
}
