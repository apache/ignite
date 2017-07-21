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

package org.apache.ignite.internal.util;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;

/**
 * Synchronization aid to track "busy" state of a subsystem that owns it.
 * <p>
 * For example, there may be a manager that have different threads for some
 * purposes and the manager must not be stopped while at least a single thread
 * is in "busy" state. In this situation each thread must enter to "busy"
 * state calling method {@link #enterBusy()} in critical pieces of code
 * which, i.e. use grid kernal functionality, notifying that the manager
 * and the whole grid kernal cannot be stopped while it's in progress. Once
 * the activity is done, the thread should leave "busy" state calling method
 * {@link #leaveBusy()}. The manager itself, when stopping, should call method
 * {@link #block} that blocks till all activities leave "busy" state.
 */
@GridToStringExclude
public class GridBusyLock {
    /** Underlying re-entrant read-write lock. */
    @SuppressWarnings("TypeMayBeWeakened")
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Enters "busy" state.
     *
     * @return {@code true} if entered to busy state.
     */
    public boolean enterBusy() {
        return !lock.writeLock().isHeldByCurrentThread() && lock.readLock().tryLock();
    }

    /**
     * Checks if busy lock was blocked by current thread.
     *
     * @return {@code True} if busy lock was blocked by current thread.
     */
    public boolean blockedByCurrentThread() {
        return lock.writeLock().isHeldByCurrentThread();
    }

    /**
     * Leaves "busy" state.
     */
    public void leaveBusy() {
        lock.readLock().unlock();
    }

    /**
     * Blocks current thread till all activities left "busy" state
     * and prevents them from further entering to "busy" state.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public void block() {
        lock.writeLock().lock();
    }

    /**
     * Makes possible for activities entering busy state again.
     */
    public void unblock() {
        lock.writeLock().unlock();
    }
}
