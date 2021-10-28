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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link IgniteSpinReadWriteLock}.
 */
@Timeout(20)
class IgniteSpinReadWriteLockTest {
    /** The lock under test. */
    private final IgniteSpinReadWriteLock lock = new IgniteSpinReadWriteLock();

    /** Executor service used to run tasks in threads different from the main test thread. */
    private final ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * Cleans up after a test.
     *
     */
    @AfterEach
    void cleanup() {
        releaseReadLockHeldByCurrentThread();
        releaseWriteLockHeldByCurrentThread();

        IgniteUtils.shutdownAndAwaitTermination(executor, 3, TimeUnit.SECONDS);
    }

    /***/
    private void releaseReadLockHeldByCurrentThread() {
        while (true) {
            try {
                lock.readUnlock();
            }
            catch (IllegalMonitorStateException e) {
                // released our read lock completely
                break;
            }
        }
    }

    /***/
    private void releaseWriteLockHeldByCurrentThread() {
        while (lock.writeLockedByCurrentThread())
            lock.writeUnlock();
    }

    /**
     *
     */
    @Test
    void readLockDoesNotAllowWriteLockToBeAcquired() {
        lock.readLock();

        assertThatWriteLockAcquireAttemptBlocksForever();

        lock.readUnlock();
    }

    /***/
    private void assertThatWriteLockAcquireAttemptBlocksForever() {
        Future<?> future = executor.submit(lock::writeLock);

        assertThrows(TimeoutException.class, () -> future.get(500, TimeUnit.MILLISECONDS));
    }

    /**
     *
     */
    @Test
    void readLockDoesNotAllowWriteLockWithoutSleepsToBeAcquired() {
        lock.readLock();

        assertThatWriteLockAcquireAttemptWithoutSleepsBlocksForever();

        lock.readUnlock();
    }

    /**
     *
     */
    @Test
    void readLockDoesNotAllowWriteLockToBeAcquiredWithTimeout() throws Exception {
        lock.readLock();

        Boolean acquired = callWithTimeout(() -> lock.tryWriteLock(1, TimeUnit.MILLISECONDS));
        assertThat(acquired, is(false));

        lock.readUnlock();
    }

    /**
     *
     */
    @Test
    void readLockAllowsReadLockToBeAcquired() throws Exception {
        lock.readLock();

        assertThatReadLockCanBeAcquired();
    }

    /***/
    private void assertThatReadLockCanBeAcquired() throws InterruptedException, ExecutionException, TimeoutException {
        runWithTimeout(lock::readLock);
    }

    /***/
    private <T> T callWithTimeout(Callable<T> call) throws ExecutionException, InterruptedException, TimeoutException {
        Future<T> future = executor.submit(call);
        return getWithTimeout(future);
    }

    /***/
    private void runWithTimeout(Runnable runnable) throws ExecutionException, InterruptedException, TimeoutException {
        Future<?> future = executor.submit(runnable);
        getWithTimeout(future);
    }

    /***/
    private static <T> T getWithTimeout(Future<? extends T> future) throws ExecutionException,
            InterruptedException, TimeoutException {
        return future.get(10, TimeUnit.SECONDS);
    }

    /**
     *
     */
    @Test
    void writeLockDoesNotAllowReadLockToBeAcquired() {
        lock.writeLock();

        assertThatReadLockAcquireAttemptBlocksForever();

        lock.writeUnlock();
    }

    /***/
    private void assertThatReadLockAcquireAttemptBlocksForever() {
        Future<?> readLockAttemptFuture = executor.submit(lock::readLock);

        assertThrows(TimeoutException.class, () -> readLockAttemptFuture.get(500, TimeUnit.MILLISECONDS));
    }

    /**
     *
     */
    @Test
    void writeLockDoesNotAllowWriteLockToBeAcquired() {
        lock.writeLock();

        assertThatWriteLockAcquireAttemptBlocksForever();

        lock.writeUnlock();
    }

    /**
     *
     */
    @Test
    void writeLockAcquiredWithoutSleepsDoesNotAllowReadLockToBeAcquired() {
        lock.writeLockBusy();

        assertThatReadLockAcquireAttemptBlocksForever();

        lock.writeUnlock();
    }

    /**
     *
     */
    @Test
    void writeLockAcquiredWithoutSleepsDoesNotAllowWriteLockToBeAcquired() {
        lock.writeLockBusy();

        assertThatWriteLockAcquireAttemptBlocksForever();

        lock.writeUnlock();
    }

    /**
     *
     */
    @Test
    void writeLockDoesNotAllowWriteLockWithoutSleepsToBeAcquired() {
        lock.writeLock();

        assertThatWriteLockAcquireAttemptWithoutSleepsBlocksForever();

        lock.writeUnlock();
    }

    /***/
    private void assertThatWriteLockAcquireAttemptWithoutSleepsBlocksForever() {
        Future<?> future = executor.submit(lock::writeLockBusy);

        assertThrows(TimeoutException.class, () -> future.get(500, TimeUnit.MILLISECONDS));
    }

    /**
     *
     */
    @Test
    void writeLockAcquiredWithoutSleepsDoesNotAllowWriteLockWithoutSleepsToBeAcquired() {
        lock.writeLockBusy();

        assertThatWriteLockAcquireAttemptWithoutSleepsBlocksForever();

        lock.writeUnlock();
    }

    /***/
    @Test
    void readUnlockReleasesTheLock() throws Exception {
        lock.readLock();
        lock.readUnlock();

        runWithTimeout(lock::writeLock);
    }

    /***/
    @Test
    void writeUnlockReleasesTheLock() throws Exception {
        lock.writeLock();
        lock.writeUnlock();

        assertThatReadLockCanBeAcquired();
    }

    /***/
    @Test
    void writeUnlockReleasesTheLockTakenWithoutSleeps() throws Exception {
        lock.writeLockBusy();
        lock.writeUnlock();

        assertThatReadLockCanBeAcquired();
    }

    /**
     *
     */
    @Test
    void testWriteLockReentry() {
        lock.writeLock();

        lock.writeLock();

        assertTrue(lock.tryWriteLock());
    }

    /**
     *
     */
    @Test
    void testWriteLockReentryWithoutSleeps() {
        lock.writeLockBusy();

        lock.writeLockBusy();

        assertTrue(lock.tryWriteLock());
    }

    /**
     *
     */
    @Test
    void testWriteLockReentryWithTryWriteLock() {
        lock.tryWriteLock();

        assertTrue(lock.tryWriteLock());
    }

    /**
     *
     */
    @Test
    void testWriteLockReentryWithTimeout() throws Exception {
        lock.tryWriteLock(1, TimeUnit.MILLISECONDS);
        lock.tryWriteLock(1, TimeUnit.MILLISECONDS);

        assertTrue(lock.tryWriteLock());
    }

    /**
     *
     */
    @Test
    void testReadLockReentry() {
        lock.readLock();

        lock.readLock();

        assertTrue(lock.tryReadLock());
    }

    /**
     * @throws Exception If any error occurs.
     */
    @Test
    void testReadLockReentryWhenConcurrentAttemptToAcquireWriteLockHappens() throws Exception {
        lock.readLock();

        Future<?> future = executor.submit(() -> {
            assertFalse(lock.tryWriteLock());

            lock.writeLock();
        });

        waitTillWriteLockAcquireAttemptIsInitiated();

        lock.readLock();

        assertTrue(lock.tryReadLock());

        lock.readUnlock();
        lock.readUnlock();
        lock.readUnlock();

        future.get(1, TimeUnit.SECONDS);
    }

    /***/
    private void waitTillWriteLockAcquireAttemptIsInitiated() throws InterruptedException {
        boolean sawAnAttempt = IgniteTestUtils.waitForCondition(
                () -> lock.pendingWriteLocksCount() > 0, TimeUnit.SECONDS.toMillis(10));
        assertTrue(sawAnAttempt, "Did not see any attempt to acquire write lock");
    }

    /**
     *
     */
    @Test
    void shouldAllowAcquireAndReleaseReadLockWhileHoldingWriteLock() {
        lock.writeLock();

        lock.readLock();
        lock.readUnlock();

        lock.writeUnlock();
    }

    /**
     *
     */
    @Test
    void shouldAllowInterleavingHoldingReadAndWriteLocks() {
        lock.writeLock();

        lock.readLock();

        lock.writeUnlock();

        assertFalse(lock.tryWriteLock());

        lock.readUnlock();

        // Test that we can operate with write locks now.
        lock.writeLock();
        lock.writeUnlock();
    }

    /**
     *
     */
    @Test
    void readLockReleasedLessTimesThanAcquiredShouldStillBeTaken() {
        lock.readLock();
        lock.readLock();
        lock.readUnlock();

        assertThatWriteLockAcquireAttemptBlocksForever();

        lock.readUnlock();
    }

    /**
     *
     */
    @Test
    void writeLockReleasedLessTimesThanAcquiredShouldStillBeTaken() {
        lock.writeLock();
        lock.writeLock();
        lock.writeUnlock();

        assertThatReadLockAcquireAttemptBlocksForever();

        lock.writeUnlock();
    }

    /**
     *
     */
    @Test
    void shouldThrowOnReadUnlockingWhenNotHoldingReadLock() {
        assertThrows(IllegalMonitorStateException.class, lock::readUnlock);
    }

    /**
     *
     */
    @Test
    void shouldThrowOnWriteUnlockingWhenNotHoldingWriteLock() {
        assertThrows(IllegalMonitorStateException.class, lock::writeUnlock);
    }

    /**
     *
     */
    @Test
    void readLockAcquiredWithTryReadLockDoesNotAllowWriteLockToBeAcquired() {
        lock.tryReadLock();

        assertThatWriteLockAcquireAttemptBlocksForever();

        lock.readUnlock();
    }

    /**
     */
    @Test
    void tryReadLockShouldReturnTrueWhenReadLockWasAcquiredSuccessfully() {
        assertTrue(lock.tryReadLock());
    }

    /**
     */
    @Test
    void tryReadLockShouldReturnFalseWhenReadLockCouldNotBeAcquired() throws Exception {
        lock.writeLock();

        Boolean acquired = callWithTimeout(lock::tryReadLock);

        assertThat(acquired, is(false));
    }

    /**
     *
     */
    @Test
    void writeLockAcquiredWithTryWriteLockDoesNotAllowWriteLockToBeAcquired() {
        lock.tryWriteLock();

        assertThatReadLockAcquireAttemptBlocksForever();

        lock.writeUnlock();
    }

    /**
     */
    @Test
    void tryWriteLockShouldReturnTrueWhenWriteLockWasAcquiredSuccessfully() {
        assertTrue(lock.tryWriteLock());
    }

    /**
     */
    @Test
    void tryWriteLockShouldReturnFalseWhenWriteLockCouldNotBeAcquired() throws Exception {
        lock.writeLock();

        Boolean acquired = callWithTimeout(lock::tryWriteLock);

        assertThat(acquired, is(false));
    }

    /**
     *
     */
    @Test
    void writeLockedByCurrentThreadShouldReturnTrueWhenLockedByCurrentThread() {
        lock.writeLock();

        assertTrue(lock.writeLockedByCurrentThread());
    }

    /**
     *
     */
    @Test
    void writeLockedByCurrentThreadShouldReturnFalseWhenNotLocked() {
        assertFalse(lock.writeLockedByCurrentThread());
    }

    /**
     *
     */
    @Test
    void writeLockedByCurrentThreadShouldReturnFalseWhenLockedByAnotherThread() throws Exception {
        lock.writeLock();

        Boolean lockedByCaller = callWithTimeout(lock::writeLockedByCurrentThread);
        assertThat(lockedByCaller, is(false));
    }
}
