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

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.failure.FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_RELEASED;

/**
 * Checkpoint lock for outer usage which should be used to protect data during writing to memory. It contains complex
 * logic for the correct taking of inside checkpoint lock(timeout, force checkpoint, etc.).
 */
public class CheckpointTimeoutLock {
    /** Ignite logger. */
    protected final IgniteLogger log;

    /** Failure processor. */
    private final FailureProcessor failureProcessor;

    /** Data regions which should be covered by this lock.  */
    private final Supplier<Collection<DataRegion>> dataRegions;

    /** Internal checkpoint lock. */
    private final CheckpointReadWriteLock checkpointReadWriteLock;

    /** Service for triggering the checkpoint. */
    private final Checkpointer checkpointer;

    /** Timeout for checkpoint read lock acquisition in milliseconds. */
    private volatile long checkpointReadLockTimeout;

    /** Stop flag. */
    private boolean stop;

    /**
     * @param logger Logger.
     * @param processor Failure processor.
     * @param regions Data regions.
     * @param lock Checkpoint read-write lock.
     * @param checkpointer Checkpointer.
     * @param checkpointReadLockTimeout Checkpoint lock timeout.
     */
    CheckpointTimeoutLock(
        Function<Class<?>, IgniteLogger> logger,
        FailureProcessor processor,
        Supplier<Collection<DataRegion>> regions,
        CheckpointReadWriteLock lock,
        Checkpointer checkpointer,
        long checkpointReadLockTimeout
    ) {
        this.log = logger.apply(getClass());
        failureProcessor = processor;
        dataRegions = regions;
        checkpointReadWriteLock = lock;
        this.checkpointer = checkpointer;
        this.checkpointReadLockTimeout = checkpointReadLockTimeout;
    }

    /**
     * Gets the checkpoint read lock. While this lock is held, checkpoint thread will not acquireSnapshotWorker memory
     * state.
     *
     * @throws IgniteException If failed.
     */
    public void checkpointReadLock() {
        if (checkpointReadWriteLock.isWriteLockHeldByCurrentThread())
            return;

        long timeout = checkpointReadLockTimeout;

        long start = U.currentTimeMillis();

        boolean interrupted = false;

        try {
            for (; ; ) {
                try {
                    if (timeout > 0 && (U.currentTimeMillis() - start) >= timeout)
                        failCheckpointReadLock();

                    try {
                        if (timeout > 0) {
                            if (!checkpointReadWriteLock.tryReadLock(timeout - (U.currentTimeMillis() - start),
                                TimeUnit.MILLISECONDS))
                                failCheckpointReadLock();
                        }
                        else
                            checkpointReadWriteLock.readLock();
                    }
                    catch (InterruptedException e) {
                        interrupted = true;

                        continue;
                    }

                    if (stop) {
                        checkpointReadWriteLock.readUnlock();

                        throw new IgniteException(new NodeStoppingException("Failed to perform cache update: node is stopping."));
                    }

                    if (checkpointReadWriteLock.getReadHoldCount() > 1 || safeToUpdatePageMemories() || checkpointer.runner() == null)
                        break;
                    else {
                        CheckpointProgress pages = checkpointer.scheduleCheckpoint(0, "too many dirty pages");

                        checkpointReadWriteLock.readUnlock();

                        if (timeout > 0 && U.currentTimeMillis() - start >= timeout)
                            failCheckpointReadLock();

                        try {
                            pages
                                .futureFor(LOCK_RELEASED)
                                .getUninterruptibly();
                        }
                        catch (IgniteFutureTimeoutCheckedException e) {
                            failCheckpointReadLock();
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException("Failed to wait for checkpoint begin.", e);
                        }
                    }
                }
                catch (CheckpointReadLockTimeoutException e) {
                    log.error(e.getMessage(), e);

                    timeout = 0;
                }
            }
        }
        finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * @return {@code true} if all PageMemory instances are safe to update.
     */
    private boolean safeToUpdatePageMemories() {
        Collection<DataRegion> memPlcs = dataRegions.get();

        if (memPlcs == null)
            return true;

        for (DataRegion memPlc : memPlcs) {
            if (!memPlc.config().isPersistenceEnabled())
                continue;

            PageMemoryEx pageMemEx = (PageMemoryEx)memPlc.pageMemory();

            if (!pageMemEx.safeToUpdate())
                return false;
        }

        return true;
    }

    /**
     * Releases the checkpoint read lock.
     */
    public void checkpointReadUnlock() {
        checkpointReadWriteLock.readUnlock();
    }

    /**
     * Invokes critical failure processing. Always throws.
     *
     * @throws CheckpointReadLockTimeoutException If node was not invalidated as result of handling.
     * @throws IgniteException If node was invalidated as result of handling.
     */
    private void failCheckpointReadLock() throws CheckpointReadLockTimeoutException, IgniteException {
        String msg = "Checkpoint read lock acquisition has been timed out.";

        IgniteException e = new IgniteException(msg);

        if (failureProcessor.process(new FailureContext(SYSTEM_CRITICAL_OPERATION_TIMEOUT, e)))
            throw e;

        throw new CheckpointReadLockTimeoutException(msg);
    }

    /**
     * Timeout for checkpoint read lock acquisition.
     *
     * @return Timeout for checkpoint read lock acquisition in milliseconds.
     */
    public long checkpointReadLockTimeout() {
        return checkpointReadLockTimeout;
    }

    /**
     * Sets timeout for checkpoint read lock acquisition.
     *
     * @param val New timeout in milliseconds, non-positive value denotes infinite timeout.
     */
    public void checkpointReadLockTimeout(long val) {
        checkpointReadLockTimeout = val;
    }

    /**
     * @return true if checkpoint lock is held by current thread
     */
    public boolean checkpointLockIsHeldByThread() {
        return checkpointReadWriteLock.checkpointLockIsHeldByThread();
    }

    /**
     * Forbid to take this lock.
     */
    public void stop() {
        checkpointReadWriteLock.writeLock();

        try {
            stop = true;
        }
        finally {
            checkpointReadWriteLock.writeUnlock();
        }
    }

    /**
     * Prepare the lock to further usage.
     */
    public void start() {
        stop = false;
    }

    /** Indicates checkpoint read lock acquisition failure which did not lead to node invalidation. */
    private static class CheckpointReadLockTimeoutException extends IgniteCheckedException {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        private CheckpointReadLockTimeoutException(String msg) {
            super(msg);
        }
    }
}
