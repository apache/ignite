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

package org.apache.ignite.internal.processors.cache.persistence.wal.segment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class SegmentLockStorage {

    List<Consumer<Long>> observers = new ArrayList<>();

    synchronized void addObserver(Consumer<Long> observer) {
        observers.add(observer);
    }

    /**
     * Maps absolute segment index to locks counter. Lock on segment protects from archiving segment and may come from
     * {@link FileWriteAheadLogManager.RecordsIterator} during WAL replay. Map itself is guarded by <code>this</code>.
     */
    private Map<Long, Integer> locked = new HashMap<>();

    /**
     * Check if WAL segment locked (protected from move to archive)
     *
     * @param absIdx Index for check reservation.
     * @return {@code True} if index is locked.
     */
    public synchronized boolean locked(long absIdx) {
        return locked.containsKey(absIdx);
    }

    /**
     * @param absIdx Segment absolute index.
     * @return <ul><li>{@code True} if can read, no lock is held, </li><li>{@code false} if work segment, need release
     * segment later, use {@link #releaseWorkSegment} for unlock</li> </ul>
     */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    public synchronized boolean lockWorkSegment(long absIdx) {
        Integer cur = locked.get(absIdx);

        cur = cur == null ? 1 : cur + 1;

        locked.put(absIdx, cur);

        return false;
    }

    /**
     * @param absIdx Segment absolute index.
     */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    public void releaseWorkSegment(long absIdx) {
        synchronized (this) {
            Integer cur = locked.get(absIdx);

            if (cur == 1) {
                locked.remove(absIdx);

//                if (log.isDebugEnabled())
//                    log.debug("Fully released work segment (ready to archive) [absIdx=" + absIdx + ']');
            }
            else {
                locked.put(absIdx, cur - 1);

//                if (log.isDebugEnabled())
//                    log.debug("Partially released work segment [absIdx=" + absIdx + ", pins=" + (cur - 1) + ']');
            }

            observers.forEach(observer -> observer.accept(absIdx));
        }
    }

}
