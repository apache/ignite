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
    private Map<Long, Integer> locked = new ConcurrentHashMap<>();

    /**
     * Check if WAL segment locked (protected from move to archive)
     *
     * @param absIdx Index for check reservation.
     * @return {@code True} if index is locked.
     */
    public boolean locked(long absIdx) {
        return locked.containsKey(absIdx);
    }

    /**
     * @param absIdx Segment absolute index.
     * @return <ul><li>{@code True} if can read, no lock is held, </li><li>{@code false} if work segment, need release
     * segment later, use {@link #releaseWorkSegment} for unlock</li> </ul>
     */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    boolean lockWorkSegment(long absIdx) {
        locked.compute(absIdx, (idx, count) -> count == null ? 1 : count + 1);

        return false;
    }

    /**
     * @param absIdx Segment absolute index.
     */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    void releaseWorkSegment(long absIdx) {
        locked.compute(absIdx, (idx, count) -> {
            assert count != null && count >= 1 : "cur=" + count + ", absIdx=" + absIdx;

            return count == 1 ? null : count - 1;
        });

        notifyObservers(absIdx);
    }
}
