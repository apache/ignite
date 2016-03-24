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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCondition;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Cache reentrant lock self test.
 */
public abstract class IgniteLockAbstractSelfTest extends IgniteAtomicsAbstractTest
    implements Externalizable {
    /** */
    private static final int NODES_CNT = 4;

    /** */
    protected static final int THREADS_CNT = 5;

    /** */
    private static final Random RND = new Random();

    /** */
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return NODES_CNT;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLock() throws Exception {
        checkReentrantLock();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailover() throws Exception {
        if (atomicsCacheMode() == LOCAL)
            return;

        checkFailover(true);
        checkFailover(false);
    }

    /**
     * @param failoverSafe Failover safe flag.
     * @throws Exception
     */
    private void checkFailover(final boolean failoverSafe) throws Exception {
        IgniteEx g = startGrid(NODES_CNT + 1);

        // For vars locality.
        {
            // Ensure not exists.
            assert g.reentrantLock("lock", failoverSafe, false) == null;

            IgniteLock lock  = g.reentrantLock("lock", failoverSafe, true);

            lock.lock();

            assert lock.tryLock();

            assertEquals(2, lock.getHoldCount());
        }

        Ignite g0 = grid(0);

        final IgniteLock lock0 = g0.reentrantLock("lock", false, false);

        assert !lock0.tryLock();

        assertEquals(0, lock0.getHoldCount());

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try {
                        lock0.lock();

                        info("Acquired in separate thread.");

                        // Lock is acquired silently only in failoverSafe mode.
                        assertTrue(failoverSafe);

                        lock0.unlock();

                        info("Released lock in separate thread.");
                    }
                    catch (IgniteInterruptedException e) {
                        if (!failoverSafe)
                            info("Ignored expected exception: " + e);
                        else
                            throw e;
                    }
                    return null;
                }
            },
            1);

        Thread.sleep(100);

        g.close();

        fut.get(500);

        lock0.close();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkReentrantLock() throws Exception {
        // Test API.
        checkLock();

        checkFailoverSafe();

        // Test main functionality.
        IgniteLock lock1 = grid(0).reentrantLock("lock", true, true);

        assertFalse(lock1.isLocked());

        lock1.lock();

        IgniteCompute comp = grid(0).compute().withAsync();

        comp.call(new IgniteCallable<Object>() {
            @IgniteInstanceResource
            private Ignite ignite;

            @LoggerResource
            private IgniteLogger log;

            @Nullable @Override public Object call() throws Exception {
                // Test reentrant lock in multiple threads on each node.
                IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
                    new Callable<Object>() {
                        @Nullable @Override public Object call() throws Exception {
                            IgniteLock lock = ignite.reentrantLock("lock", true, true);

                            assert lock != null;

                            log.info("Thread is going to wait on reentrant lock: " + Thread.currentThread().getName());

                            assert lock.tryLock(1, MINUTES);

                            log.info("Thread is again runnable: " + Thread.currentThread().getName());

                            lock.unlock();

                            return null;
                        }
                    },
                    5,
                    "test-thread"
                );

                fut.get();

                return null;
            }
        });

        IgniteFuture<Object> fut = comp.future();

        Thread.sleep(3000);

        assert lock1.isHeldByCurrentThread() == true;

        assert lock1.getHoldCount() == 1;

        lock1.lock();

        assert lock1.isHeldByCurrentThread() == true;

        assert lock1.getHoldCount() == 2;

        lock1.unlock();

        lock1.unlock();

        // Ensure there are no hangs.
        fut.get();

        // Test operations on removed lock.
        lock1.close();

        checkRemovedReentrantLock(lock1);
    }

    /**
     * @param lock IgniteLock.
     * @throws Exception If failed.
     */
    protected void checkRemovedReentrantLock(final IgniteLock lock) throws Exception {
        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return lock.removed();
            }
        }, 5000);

        assert lock.removed();
    }

    /**
     * This method only checks if parameter of new reentrant lock is initialized properly.
     * For tests considering failure recovery see @GridCachePartitionedNodeFailureSelfTest.
     *
     * @throws Exception Exception.
     */
    private void checkFailoverSafe() throws Exception {
        // Checks only if reentrant lock is initialized properly
        IgniteLock lock = createReentrantLock("rmv", true);

        assert lock.isFailoverSafe();

        removeReentrantLock("rmv");

        IgniteLock lock1 = createReentrantLock("rmv1", false);

        assert !lock1.isFailoverSafe();

        removeReentrantLock("rmv1");
    }

    /**
     * @throws Exception Exception.
     */
    private void checkLock() throws Exception {
        // Check only 'false' cases here. Successful lock is tested over the grid.
        final IgniteLock lock = createReentrantLock("acquire", false);

        lock.lock();

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                assertNotNull(lock);

                assert !lock.tryLock();

                assert !lock.tryLock(10, MICROSECONDS);

                return null;
            }
        });

        fut.get();

        lock.unlock();

        removeReentrantLock("acquire");
    }

    /**
     * @param lockName Reentrant lock name.
     * @param failoverSafe Fairness flag.
     * @return New distributed reentrant lock.
     * @throws Exception If failed.
     */
    private IgniteLock createReentrantLock(String lockName, boolean failoverSafe)
        throws Exception {
        IgniteLock lock = grid(RND.nextInt(NODES_CNT)).reentrantLock(lockName, failoverSafe, true);

        // Test initialization.
        assert lockName.equals(lock.name());
        assert lock.isLocked() == false;
        assert lock.isFailoverSafe() == failoverSafe;

        return lock;
    }

    /**
     * @param lockName Reentrant lock name.
     * @throws Exception If failed.
     */
    private void removeReentrantLock(String lockName)
        throws Exception {
        IgniteLock lock = grid(RND.nextInt(NODES_CNT)).reentrantLock(lockName, false, true);

        assert lock != null;

        // Remove lock on random node.
        IgniteLock lock0 = grid(RND.nextInt(NODES_CNT)).reentrantLock(lockName, false, true);

        assertNotNull(lock0);

        lock0.close();

        // Ensure reentrant lock is removed on all nodes.
        for (Ignite g : G.allGrids())
            assertNull(((IgniteKernal)g).context().dataStructures().reentrantLock(lockName, false, false));

        checkRemovedReentrantLock(lock);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinode1() throws Exception {
        if (gridCount() == 1)
            return;

        IgniteLock lock = grid(0).reentrantLock("s1", true, true);

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < gridCount(); i++) {
            final Ignite ignite = grid(i);

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteLock lock = ignite.reentrantLock("s1", true, false);

                    assertNotNull(lock);

                    IgniteCondition cond1 = lock.getOrCreateCondition("c1");

                    IgniteCondition cond2 = lock.getOrCreateCondition("c2");

                    try {
                        boolean wait = lock.tryLock(30_000, MILLISECONDS);

                        assertTrue(wait);

                        cond2.signal();

                        cond1.await();
                    }
                    finally {
                        lock.unlock();
                    }

                    return null;
                }
            }));
        }

        boolean done = false;

        while(!done) {
            done = true;

            for (IgniteInternalFuture<?> fut : futs){
                if(!fut.isDone())
                    done = false;
            }

            try{
                lock.lock();

                lock.getOrCreateCondition("c1").signal();

                lock.getOrCreateCondition("c2").await(10,MILLISECONDS);
            }
            finally {
                lock.unlock();
            }
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(30_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockInterruptibly() throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<Thread>();

        lock0.lock();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertFalse(lock0.isHeldByCurrentThread());

                    startedThreads.add(Thread.currentThread());

                    boolean isInterrupted = false;

                    try {
                        lock0.lockInterruptibly();
                    }
                    catch (IgniteInterruptedException e) {
                        assertFalse(Thread.currentThread().isInterrupted());

                        isInterrupted = true;
                    }
                    finally {
                        System.out.println(Thread.currentThread());

                        // Assert that thread was interrupted.
                        assertTrue(isInterrupted);

                        // Assert that locked is still owned by main thread.
                        assertTrue(lock0.isLocked());

                        // Assert that this thread doesn't own the lock.
                        assertFalse(lock0.isHeldByCurrentThread());
                    }

                    return null;
                }
            }, totalThreads);

        // Wait for all threads to attempt to acquire lock.
        while (startedThreads.size() != totalThreads) {
            Thread.sleep(1000);
        }

        for (Thread t : startedThreads)
            t.interrupt();

        fut.get();

        lock0.unlock();

        assertFalse(lock0.isLocked());

        for (Thread t : startedThreads)
            assertFalse(lock0.hasQueuedThread(t));

        lock0.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLock() throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<Thread>();

        lock0.lock();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertFalse(lock0.isHeldByCurrentThread());

                    startedThreads.add(Thread.currentThread());

                    boolean isInterrupted = false;

                    try {
                        lock0.lock();
                    }
                    catch (IgniteInterruptedException e) {
                        isInterrupted = true;

                        fail("Lock() method is uninterruptible.");
                    }
                    finally {
                        // Assert that thread was not interrupted.
                        assertFalse(isInterrupted);

                        // Assert that interrupted flag is set and clear it in order to call unlock().
                        assertTrue(Thread.interrupted());

                        // Assert that lock is still owned by this thread.
                        assertTrue(lock0.isLocked());

                        // Assert that this thread does own the lock.
                        assertTrue(lock0.isHeldByCurrentThread());

                        // Release lock.
                        lock0.unlock();
                    }

                    return null;
                }
            }, totalThreads);

        // Wait for all threads to attempt to acquire lock.
        while (startedThreads.size() != totalThreads) {
            Thread.sleep(500);
        }

        for (Thread t : startedThreads)
            t.interrupt();

        lock0.unlock();

        fut.get();

        assertFalse(lock0.isLocked());

        for (Thread t : startedThreads)
            assertFalse(lock0.hasQueuedThread(t));

        lock0.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTryLock() throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<Thread>();

        lock0.lock();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertFalse(lock0.isHeldByCurrentThread());

                    startedThreads.add(Thread.currentThread());

                    boolean isInterrupted = false;

                    boolean locked = false;

                    try {
                        locked = lock0.tryLock();
                    }
                    catch (IgniteInterruptedException e) {
                        isInterrupted = true;

                        fail("tryLock() method is uninterruptible.");
                    }
                    finally {
                        // Assert that thread was not interrupted.
                        assertFalse(isInterrupted);

                        // Assert that lock is locked.
                        assertTrue(lock0.isLocked());

                        // Assert that this thread does own the lock.
                        assertEquals(locked, lock0.isHeldByCurrentThread());

                        // Release lock.
                        if (locked)
                            lock0.unlock();
                    }

                    return null;
                }
            }, totalThreads);

        // Wait for all threads to attempt to acquire lock.
        while (startedThreads.size() != totalThreads) {
            Thread.sleep(500);
        }

        for (Thread t : startedThreads)
            t.interrupt();

        fut.get();

        lock0.unlock();

        assertFalse(lock0.isLocked());

        for (Thread t : startedThreads)
            assertFalse(lock0.hasQueuedThread(t));

        lock0.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTryLockTimed() throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<Thread>();

        lock0.lock();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertFalse(lock0.isHeldByCurrentThread());

                    startedThreads.add(Thread.currentThread());

                    boolean isInterrupted = false;

                    boolean locked = false;

                    try {
                        locked = lock0.tryLock(100, TimeUnit.MILLISECONDS);
                    }
                    catch (IgniteInterruptedException e) {
                        isInterrupted = true;
                    }
                    finally {
                        // Assert that thread was not interrupted.
                        assertFalse(isInterrupted);

                        // Assert that tryLock returned false.
                        assertFalse(locked);

                        // Assert that lock is still owned by main thread.
                        assertTrue(lock0.isLocked());

                        // Assert that this thread doesn't own the lock.
                        assertFalse(lock0.isHeldByCurrentThread());

                        // Release lock.
                        if (locked)
                            lock0.unlock();
                    }

                    return null;
                }
            }, totalThreads);

        fut.get();

        lock0.unlock();

        assertFalse(lock0.isLocked());

        for (Thread t : startedThreads)
            assertFalse(lock0.hasQueuedThread(t));

        lock0.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConditionAwaitUninterruptibly() throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<Thread>();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertFalse(lock0.isHeldByCurrentThread());

                    startedThreads.add(Thread.currentThread());

                    boolean isInterrupted = false;

                    lock0.lock();

                    IgniteCondition cond = lock0.getOrCreateCondition("cond");

                    try {
                        cond.awaitUninterruptibly();
                    }
                    catch (IgniteInterruptedException e) {
                        isInterrupted = true;
                    }
                    finally {
                        // Assert that thread was not interrupted.
                        assertFalse(isInterrupted);

                        // Assert that lock is still locked.
                        assertTrue(lock0.isLocked());

                        // Assert that this thread does own the lock.
                        assertTrue(lock0.isHeldByCurrentThread());

                        // Clear interrupt flag.
                        assertTrue(Thread.interrupted());

                        // Release lock.
                        if (lock0.isHeldByCurrentThread())
                            lock0.unlock();
                    }

                    return null;
                }
            }, totalThreads);

        // Wait for all threads to attempt to acquire lock.
        while (startedThreads.size() != totalThreads) {
            Thread.sleep(500);
        }

        lock0.lock();

        for (Thread t : startedThreads) {
            t.interrupt();

            lock0.getOrCreateCondition("cond").signal();
        }

        lock0.unlock();

        fut.get();

        assertFalse(lock0.isLocked());

        for (Thread t : startedThreads)
            assertFalse(lock0.hasQueuedThread(t));

        lock0.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConditionInterruptAwait() throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<Thread>();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertFalse(lock0.isHeldByCurrentThread());

                    startedThreads.add(Thread.currentThread());

                    boolean isInterrupted = false;

                    lock0.lock();

                    IgniteCondition cond = lock0.getOrCreateCondition("cond");

                    try {
                        cond.await();
                    }
                    catch (IgniteInterruptedException e) {
                        isInterrupted = true;
                    }
                    finally {
                        // Assert that thread was interrupted.
                        assertTrue(isInterrupted);

                        // Assert that lock is still locked.
                        assertTrue(lock0.isLocked());

                        // Assert that this thread does own the lock.
                        assertTrue(lock0.isHeldByCurrentThread());

                        // Release lock.
                        if (lock0.isHeldByCurrentThread())
                            lock0.unlock();
                    }

                    return null;
                }
            }, totalThreads);

        // Wait for all threads to attempt to acquire lock.
        while (startedThreads.size() != totalThreads) {
            Thread.sleep(500);
        }

        for (Thread t : startedThreads)
            t.interrupt();

        fut.get();

        assertFalse(lock0.isLocked());

        for (Thread t : startedThreads)
            assertFalse(lock0.hasQueuedThread(t));

        lock0.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testHasQueuedThreads() throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 5;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<Thread>();

        final Set<Thread> finishedThreads = new GridConcurrentHashSet<Thread>();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertFalse(lock0.isHeldByCurrentThread());

                    startedThreads.add(Thread.currentThread());

                    lock0.lock();

                    // Wait until every thread tries to lock.
                    do {
                        Thread.sleep(1000);
                    }
                    while (startedThreads.size() != totalThreads);

                    try {
                        info("Acquired in separate thread. ");

                        assertTrue(lock0.isHeldByCurrentThread());

                        assertFalse(lock0.hasQueuedThread(Thread.currentThread()));

                        finishedThreads.add(Thread.currentThread());

                        if (startedThreads.size() != finishedThreads.size()) {
                            assertTrue(lock0.hasQueuedThreads());
                        }

                        for (Thread t : startedThreads) {
                            assertTrue(lock0.hasQueuedThread(t) != finishedThreads.contains(t));
                        }
                    }
                    finally {
                        lock0.unlock();

                        assertFalse(lock0.isHeldByCurrentThread());
                    }

                    return null;
                }
            }, totalThreads);

        fut.get();

        assertFalse(lock0.hasQueuedThreads());

        for (Thread t : startedThreads)
            assertFalse(lock0.hasQueuedThread(t));

        lock0.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testHasConditionQueuedThreads() throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 5;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<Thread>();

        final Set<Thread> finishedThreads = new GridConcurrentHashSet<Thread>();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    assertFalse(lock0.isHeldByCurrentThread());

                    IgniteCondition cond = lock0.getOrCreateCondition("cond");

                    lock0.lock();

                    startedThreads.add(Thread.currentThread());

                    // Wait until every thread tries to lock.
                    do {
                        cond.await();

                        Thread.sleep(1000);
                    }
                    while (startedThreads.size() != totalThreads);

                    try {
                        info("Acquired in separate thread. Number of threads waiting on condition: "
                            + lock0.getWaitQueueLength(cond));

                        assertTrue(lock0.isHeldByCurrentThread());

                        assertFalse(lock0.hasQueuedThread(Thread.currentThread()));

                        finishedThreads.add(Thread.currentThread());

                        if (startedThreads.size() != finishedThreads.size()) {
                            assertTrue(lock0.hasWaiters(cond));
                        }

                        for (Thread t : startedThreads) {
                            if (!finishedThreads.contains(t))
                                assertTrue(lock0.hasWaiters(cond));
                        }

                        assertTrue(lock0.getWaitQueueLength(cond) == (startedThreads.size() - finishedThreads.size()));
                    }
                    finally {
                        cond.signal();

                        lock0.unlock();

                        assertFalse(lock0.isHeldByCurrentThread());
                    }

                    return null;
                }
            }, totalThreads);

        IgniteCondition cond = lock0.getOrCreateCondition("cond");

        lock0.lock();

        try {
            // Wait until all threads are waiting on condition.
            while (lock0.getWaitQueueLength(cond) != totalThreads) {
                lock0.unlock();

                Thread.sleep(1000);

                lock0.lock();
            }

            // Signal once to get things started.
            cond.signal();
        }
        finally {
            lock0.unlock();
        }

        fut.get();

        assertFalse(lock0.hasQueuedThreads());

        for (Thread t : startedThreads)
            assertFalse(lock0.hasQueuedThread(t));

        lock0.close();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }
}
