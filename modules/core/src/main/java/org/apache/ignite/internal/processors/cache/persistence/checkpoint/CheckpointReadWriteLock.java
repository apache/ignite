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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.ReentrantReadWriteLockWithTracking;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS;

/**
 * Wrapper of the classic read write lock with checkpoint features.
 */
public class CheckpointReadWriteLock {
    /** Assertion enabled. */
    private static final boolean ASSERTION_ENABLED = GridCacheDatabaseSharedManager.class.desiredAssertionStatus();

    /** Checkpoint lock hold count. */
    public static final ThreadLocal<Integer> CHECKPOINT_LOCK_HOLD_COUNT = ThreadLocal.withInitial(() -> 0);

    /**
     * Any thread with a such prefix is managed by the checkpoint. So some conditions can rely on it(ex. we don't need a
     * checkpoint lock there because checkpoint is already held the write lock).
     */
    static final String CHECKPOINT_RUNNER_THREAD_PREFIX = "checkpoint-runner";

    /** Checkpont lock. */
    private final ReentrantReadWriteLockWithTracking checkpointLock;

    /**
     * @param logger Logger.
     */
    CheckpointReadWriteLock(Function<Class<?>, IgniteLogger> logger) {
        if (getBoolean(IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS))
            checkpointLock = new ReentrantReadWriteLockWithTracking(logger.apply(getClass()), 5_000);
        else
            checkpointLock = new ReentrantReadWriteLockWithTracking();
    }

    /**
     * Gets the checkpoint read lock. While this lock is held, checkpoint thread will not acquireSnapshotWorker memory
     * state.
     *
     * @throws IgniteException If failed.
     */
    public void readLock() {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return;

        checkpointLock.readLock().lock();

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() + 1);
    }

    /**
     * Gets the checkpoint read lock. While this lock is held, checkpoint thread will not acquireSnapshotWorker memory
     * state.
     *
     * @throws IgniteException If failed.
     */
    public boolean tryReadLock(long timeout, TimeUnit unit) throws InterruptedException {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return true;

        boolean res = checkpointLock.readLock().tryLock(timeout, unit);

        if (ASSERTION_ENABLED && res)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() + 1);

        return res;
    }

    /**
     * This method works only if the assertion is enabled or it always returns true otherwise.
     *
     * @return {@code true} if checkpoint lock is held by current thread.
     */
    public boolean checkpointLockIsHeldByThread() {
        return !ASSERTION_ENABLED ||
            checkpointLock.isWriteLockedByCurrentThread() ||
            CHECKPOINT_LOCK_HOLD_COUNT.get() > 0 ||
            Thread.currentThread().getName().startsWith(CHECKPOINT_RUNNER_THREAD_PREFIX);
    }

    /**
     * Releases the checkpoint read lock.
     */
    public void readUnlock() {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return;

        checkpointLock.readLock().unlock();

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() - 1);
    }

    /**
     * Take the checkpoint write lock.
     */
    public void writeLock() {
        checkpointLock.writeLock().lock();
    }

    /**
     * @return {@code true} if current thread hold the write lock.
     */
    public boolean isWriteLockHeldByCurrentThread() {
        return checkpointLock.writeLock().isHeldByCurrentThread();
    }

    /**
     * Release the checkpoint write lock
     */
    public void writeUnlock() {
        checkpointLock.writeLock().unlock();
    }

    /**
     *
     */
    public int getReadHoldCount() {
        return checkpointLock.getReadHoldCount();
    }
}
