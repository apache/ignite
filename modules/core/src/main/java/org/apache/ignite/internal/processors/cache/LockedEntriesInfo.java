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

package org.apache.ignite.internal.processors.cache;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class to acquire java level locks on unordered set of entries and avoid deadlocks.
 */
public class LockedEntriesInfo {
    /** Deadlock detection timeout in milliseconds. */
    private static final long DEADLOCK_DETECTION_TIMEOUT = 500L;

    /** Locked entries info for each thread. */
    private final Map<Long, LockedEntries> lockedEntriesPerThread = new ConcurrentHashMap<>();

    /**
     * Make an attempt to lock the entries and updates per-thread locked entries info. If lock attempt is successful,
     * locked entries info for the current thread should be removed later by {@link #removeForCurrentThread} method.
     *
     * @param entries Entries to lock.
     * @return {@code True} if entries were successfully locked, {@code false} if possible deadlock detected or
     *      some entries are obsolete (lock attempt should be retried in this case).
     */
    public boolean tryLockEntries(GridCacheEntryEx[] entries) {
        LockedEntries lockedEntries = lockedEntriesPerThread.computeIfAbsent(Thread.currentThread().getId(),
                threadId -> new LockedEntries(entries));

        boolean wasInterrupted = false;

        for (int i = 0; i < entries.length; i++) {
            GridCacheEntryEx entry = entries[i];

            if (entry == null)
                continue;

            boolean retry = false;

            while (true) {
                if (entry.tryLockEntry(DEADLOCK_DETECTION_TIMEOUT))
                    break; // Successfully locked.
                else {
                    wasInterrupted |= Thread.interrupted(); // Clear thread interruption flag.

                    if (hasLockCollisions(entry, lockedEntries)) {
                        // Possible deadlock detected, unlock all locked entries and retry again.
                        retry = true;

                        break;
                    }
                    // Possible deadlock not detected, just retry lock on current entry.
                }
            }

            if (!retry && entry.obsolete()) {
                entry.unlockEntry();

                retry = true;
            }

            if (retry) {
                lockedEntries.lockedIdx = -1;

                // Unlock all previously locked.
                for (int j = 0; j < i; j++) {
                    if (entries[j] != null)
                        entries[j].unlockEntry();
                }

                if (wasInterrupted)
                    Thread.currentThread().interrupt();

                return false;
            }

            lockedEntries.lockedIdx = i;
        }

        if (wasInterrupted)
            Thread.currentThread().interrupt();

        return true;
    }

    /**
     * Remove locked entries info for current thread.
     */
    public void removeForCurrentThread() {
        lockedEntriesPerThread.remove(Thread.currentThread().getId());
    }

    /**
     * @param entry Entry.
     * @param curLockedEntries Current locked entries info.
     * @return {@code True} if another thread holds lock for this entry and started to lock entries earlier.
     */
    private boolean hasLockCollisions(GridCacheEntryEx entry, LockedEntries curLockedEntries) {
        for (Map.Entry<Long, LockedEntries> other : lockedEntriesPerThread.entrySet()) {
            LockedEntries otherLockedEntries = other.getValue();

            if (otherLockedEntries == curLockedEntries || otherLockedEntries.ts > curLockedEntries.ts)
                // Skip current thread and threads started to lock after the current thread.
                continue;

            GridCacheEntryEx[] otherThreadLocks = otherLockedEntries.entries.get();

            int otherThreadLockedIdx = otherLockedEntries.lockedIdx;

            if (otherThreadLocks == null) { // In case of thread fail.
                lockedEntriesPerThread.remove(other.getKey());

                continue;
            }

            // Visibility guarantees provided by volatile lockedIdx field.
            for (int i = 0; i <= otherThreadLockedIdx; i++) {
                if (otherThreadLocks[i] == entry)
                    return true;
            }
        }

        return false;
    }

    /** Per-thread locked entries info. */
    private static class LockedEntries {
        /** Timestamp of lock. */
        private final long ts = System.nanoTime();

        /** Entries to lock. */
        private final WeakReference<GridCacheEntryEx[]> entries;

        /** Current locked entry index. */
        private volatile int lockedIdx = -1;

        /** */
        private LockedEntries(GridCacheEntryEx[] entries) {
            this.entries = new WeakReference<>(entries);
        }
    }
}
