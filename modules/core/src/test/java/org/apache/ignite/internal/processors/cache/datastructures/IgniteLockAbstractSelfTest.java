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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCondition;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

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
        checkReentrantLock(false);

        checkReentrantLock(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailover() throws Exception {
        if (atomicsCacheMode() == LOCAL)
            return;

        checkFailover(true, false);

        checkFailover(false, false);

        checkFailover(true, true);

        checkFailover(false, true);
    }

    /**
     * Implementation of ignite data structures internally uses special system caches, need make sure
     * that transaction on these system caches do not intersect with transactions started by user.
     *
     * @throws Exception If failed.
     */
    public void testIsolation() throws Exception {
        Ignite ignite = grid(0);

        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName("myCache");
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cfg);

        try {
            IgniteLock lock = ignite.reentrantLock("lock", true, true, true);

            try (Transaction tx = ignite.transactions().txStart()) {
                cache.put(1, 1);

                boolean success = lock.tryLock(1, MILLISECONDS);

                assertTrue(success);

                tx.rollback();
            }

            assertEquals(0, cache.size());

            assertTrue(lock.isLocked());

            lock.unlock();

            assertFalse(lock.isLocked());

            lock.close();

            assertTrue(lock.removed());
        }
        finally {
            ignite.destroyCache(cfg.getName());
        }
    }

    /**
     * @param failoverSafe Failover safe flag.
     * @throws Exception If failed.
     */
    private void checkFailover(final boolean failoverSafe, final boolean fair) throws Exception {
        IgniteEx g = startGrid(NODES_CNT + 1);

        // For vars locality.
        {
            // Ensure not exists.
            assert g.reentrantLock("lock", failoverSafe, fair, false) == null;

            IgniteLock lock  = g.reentrantLock("lock", failoverSafe, fair, true);

            lock.lock();

            assert lock.tryLock();

            assertEquals(2, lock.getHoldCount());
        }

        Ignite g0 = grid(0);

        final IgniteLock lock0 = g0.reentrantLock("lock", false, fair, false);

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
                    catch (IgniteException e) {
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
    private void checkReentrantLock(final boolean fair) throws Exception {
        // Test API.
        checkLock(fair);

        checkFailoverSafe(fair);

        // Test main functionality.
        IgniteLock lock1 = grid(0).reentrantLock("lock", true, fair, true);

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
                            IgniteLock lock = ignite.reentrantLock("lock", true, fair, true);

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

        assert lock1.isHeldByCurrentThread();

        assert lock1.getHoldCount() == 1;

        lock1.lock();

        assert lock1.isHeldByCurrentThread();

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
    private void checkFailoverSafe(final boolean fair) throws Exception {
        // Checks only if reentrant lock is initialized properly
        IgniteLock lock = createReentrantLock("rmv", true, fair);

        assert lock.isFailoverSafe();

        removeReentrantLock("rmv", fair);

        IgniteLock lock1 = createReentrantLock("rmv1", false, fair);

        assert !lock1.isFailoverSafe();

        removeReentrantLock("rmv1", fair);
    }

    /**
     * @throws Exception Exception.
     */
    private void checkLock(final boolean fair) throws Exception {
        // Check only 'false' cases here. Successful lock is tested over the grid.
        final IgniteLock lock = createReentrantLock("acquire", false, fair);

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

        removeReentrantLock("acquire", fair);
    }

    /**
     * @param lockName Reentrant lock name.
     * @param failoverSafe FailoverSafe flag.
     * @param fair Fairness flag.
     * @return New distributed reentrant lock.
     * @throws Exception If failed.
     */
    private IgniteLock createReentrantLock(String lockName, boolean failoverSafe, boolean fair)
        throws Exception {
        IgniteLock lock = grid(RND.nextInt(NODES_CNT)).reentrantLock(lockName, failoverSafe, fair, true);

        // Test initialization.
        assert lockName.equals(lock.name());
        assert !lock.isLocked();
        assert lock.isFailoverSafe() == failoverSafe;
        assert lock.isFair() == fair;

        return lock;
    }

    /**
     * @param lockName Reentrant lock name.
     * @throws Exception If failed.
     */
    private void removeReentrantLock(String lockName, final boolean fair)
        throws Exception {
        IgniteLock lock = grid(RND.nextInt(NODES_CNT)).reentrantLock(lockName, false, fair, true);

        assert lock != null;

        // Remove lock on random node.
        IgniteLock lock0 = grid(RND.nextInt(NODES_CNT)).reentrantLock(lockName, false, fair, true);

        assertNotNull(lock0);

        lock0.close();

        // Ensure reentrant lock is removed on all nodes.
        for (Ignite g : G.allGrids())
            assertNull(((IgniteKernal)g).context().dataStructures().reentrantLock(lockName, false, fair, false));

        checkRemovedReentrantLock(lock);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockSerialization() throws Exception {
        final IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

        info("Lock created: " + lock);

        lock.isFailoverSafe();
        lock.isFair();

        grid(ThreadLocalRandom.current().nextInt(G.allGrids().size())).compute().broadcast(new IgniteCallable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                Thread.sleep(1000);

                lock.lock();

                try {
                    info("Inside lock: " + lock.getHoldCount());
                }
                finally {
                    lock.unlock();
                }

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testInitialization() throws Exception {
        // Test #name() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            assertEquals("lock", lock.name());

            lock.close();
        }

        // Test #isFailoverSafe() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            info("Lock created: " + lock);

            assertTrue(lock.isFailoverSafe());

            lock.close();
        }

        // Test #isFair() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            assertTrue(lock.isFair());

            lock.close();
        }

        // Test #isBroken() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            assertFalse(lock.isBroken());

            lock.close();
        }

        // Test #getOrCreateCondition(String ) method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            assertNotNull(lock.getOrCreateCondition("condition"));

            lock.close();
        }

        // Test #getHoldCount() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            assertEquals(0, lock.getHoldCount());

            lock.close();
        }

        // Test #isHeldByCurrentThread() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            assertFalse(lock.isHeldByCurrentThread());

            lock.close();
        }

        // Test #isLocked() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            assertFalse(lock.isLocked());

            lock.close();
        }

        // Test #hasQueuedThreads() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            assertFalse(lock.hasQueuedThreads());

            lock.close();
        }

        // Test #hasQueuedThread(Thread ) method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            assertFalse(lock.hasQueuedThread(Thread.currentThread()));

            lock.close();
        }

        // Test #hasWaiters(IgniteCondition ) method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            try {
                IgniteCondition cond = grid(0).reentrantLock("lock2", true, true, true).getOrCreateCondition("cond");

                lock.hasWaiters(cond);

                fail("Condition not associated with this lock passed as argument.");
            }
            catch (IllegalArgumentException ignored) {
                info("IllegalArgumentException thrown as it should be.");
            }

            try {
                IgniteCondition cond = lock.getOrCreateCondition("condition");

                lock.hasWaiters(cond);

                fail("This method should throw exception when lock is not held.");
            }
            catch (IllegalMonitorStateException ignored) {
                info("IllegalMonitorStateException thrown as it should be.");
            }

            lock.close();
        }

        // Test #getWaitQueueLength(IgniteCondition ) method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            try {
                IgniteCondition cond = grid(0).reentrantLock("lock2", true, true, true).getOrCreateCondition("cond");

                lock.getWaitQueueLength(cond);

                fail("Condition not associated with this lock passed as argument.");
            }
            catch (IllegalArgumentException ignored) {
                info("IllegalArgumentException thrown as it should be.");
            }

            try {
                IgniteCondition cond = lock.getOrCreateCondition("condition");

                lock.getWaitQueueLength(cond);

                fail("This method should throw exception when lock is not held.");
            }
            catch (IllegalMonitorStateException ignored) {
                info("IllegalMonitorStateException thrown as it should be.");
            }

            lock.close();
        }

        // Test #lock() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            lock.lock();

            lock.unlock();

            lock.close();
        }

        // Test #lockInterruptibly() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            lock.lockInterruptibly();

            lock.unlock();

            lock.close();
        }

        // Test #tryLock() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            boolean success = lock.tryLock();

            assertTrue(success);

            lock.unlock();

            lock.close();
        }

        // Test #tryLock(long, TimeUnit) method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            boolean success = lock.tryLock(1, MILLISECONDS);

            assertTrue(success);

            lock.unlock();

            lock.close();
        }

        // Test #unlock() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            try {
                lock.unlock();

                fail("This method should throw exception when lock is not held.");
            }
            catch (IllegalMonitorStateException ignored) {
                info("IllegalMonitorStateException thrown as it should be.");
            }

            lock.close();
        }

        // Test #removed() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            assertFalse(lock.removed());

            lock.close();

            assertTrue(lock.removed());
        }

        // Test #close() method.
        {
            IgniteLock lock = grid(0).reentrantLock("lock", true, true, true);

            lock.close();

            assertTrue(lock.removed());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinode1() throws Exception {
        testReentrantLockMultinode1(false);

        testReentrantLockMultinode1(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testReentrantLockMultinode1(final boolean fair) throws Exception {
        if (gridCount() == 1)
            return;

        IgniteLock lock = grid(0).reentrantLock("s1", true, fair, true);

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < gridCount(); i++) {
            final Ignite ignite = grid(i);

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteLock lock = ignite.reentrantLock("s1", true, fair, false);

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
        testLockInterruptibly(false);

        testLockInterruptibly(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testLockInterruptibly(final boolean fair) throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, fair, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<>();

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
                    catch (IgniteInterruptedException ignored) {
                        assertFalse(Thread.currentThread().isInterrupted());

                        isInterrupted = true;
                    }
                    finally {
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
    public void testLockInterruptiblyMultinode() throws Exception {
        testLockInterruptiblyMultinode(false);

        testLockInterruptiblyMultinode(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testLockInterruptiblyMultinode(final boolean fair) throws Exception {
        if (gridCount() == 1)
            return;

        // Initialize reentrant lock.
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, fair, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        lock0.lock();

        // Number of threads, one per node.
        final int threadCount = gridCount();

        final AtomicLong threadCounter = new AtomicLong(0);

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    final int localNodeId = (int)threadCounter.getAndIncrement();

                    final Ignite grid = grid(localNodeId);

                    IgniteClosure<Ignite, Void> closure = new IgniteClosure<Ignite, Void>() {
                        @Override public Void apply(Ignite ignite) {
                            final IgniteLock l = ignite.reentrantLock("lock", true, true, true);

                            final AtomicReference<Thread> thread = new AtomicReference<>();

                            final AtomicBoolean done = new AtomicBoolean(false);

                            final AtomicBoolean exceptionThrown = new AtomicBoolean(false);

                            final IgniteCountDownLatch latch = ignite.countDownLatch("latch", threadCount, false, true);

                            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
                                @Override public Void call() throws Exception {
                                    try{
                                        thread.set(Thread.currentThread());

                                        l.lockInterruptibly();
                                    }
                                    catch(IgniteInterruptedException ignored){
                                        exceptionThrown.set(true);
                                    }
                                    finally {
                                        done.set(true);
                                    }

                                    return null;
                                }
                            });

                            // Wait until l.lock() has been called.
                            while(!l.hasQueuedThreads()){
                                // No-op.
                            }

                            latch.countDown();

                            latch.await();

                            thread.get().interrupt();

                            while(!done.get()){
                                // No-op.
                            }

                            try {
                                fut.get();
                            }
                            catch (IgniteCheckedException e) {
                                fail(e.getMessage());

                                throw new RuntimeException(e);
                            }

                            assertTrue(exceptionThrown.get());

                            return null;
                        }
                    };

                    closure.apply(grid);

                    return null;
                }
            }, threadCount);

        fut.get();

        lock0.unlock();

        info("Checking if interrupted threads are removed from global waiting queue...");

        // Check if interrupted threads are removed from global waiting queue.
        boolean locked = lock0.tryLock(1000, MILLISECONDS);

        info("Interrupted threads successfully removed from global waiting queue. ");

        assertTrue(locked);

        lock0.unlock();

        assertFalse(lock0.isLocked());

        lock0.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLock() throws Exception {
        testLock(false);

        testLock(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testLock(final boolean fair) throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, fair, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<>();

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
                    catch (IgniteInterruptedException ignored) {
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
        testTryLock(false);

        testTryLock(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testTryLock(final boolean fair) throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, fair, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<>();

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
                    catch (IgniteInterruptedException ignored) {
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
        testTryLockTimed(false);

        testTryLockTimed(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testTryLockTimed(final boolean fair) throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, fair, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<>();

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
                    catch (IgniteInterruptedException ignored) {
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
        testConditionAwaitUninterruptibly(false);

        testConditionAwaitUninterruptibly(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testConditionAwaitUninterruptibly(final boolean fair) throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, fair, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<>();

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
                    catch (IgniteInterruptedException ignored) {
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
        testConditionInterruptAwait(false);

        testConditionInterruptAwait(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testConditionInterruptAwait(final boolean fair) throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, fair, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 2;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<>();

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
                    catch (IgniteInterruptedException ignored) {
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
        testHasQueuedThreads(false);

        testHasQueuedThreads(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testHasQueuedThreads(final boolean fair) throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, fair, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 5;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<>();

        final Set<Thread> finishedThreads = new GridConcurrentHashSet<>();

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
        testHasConditionQueuedThreads(false);

        testHasConditionQueuedThreads(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testHasConditionQueuedThreads(final boolean fair) throws Exception {
        final IgniteLock lock0 = grid(0).reentrantLock("lock", true, fair, true);

        assertEquals(0, lock0.getHoldCount());

        assertFalse(lock0.hasQueuedThreads());

        final int totalThreads = 5;

        final Set<Thread> startedThreads = new GridConcurrentHashSet<>();

        final Set<Thread> finishedThreads = new GridConcurrentHashSet<>();

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

    /**
     * Tests if lock is evenly acquired among nodes when fair flag is set on.
     * Since exact ordering of lock acquisitions cannot be guaranteed because it also depends
     * on the OS thread scheduling, certain deviation from uniform distribution is tolerated.
     * @throws Exception If failed.
     */
    public void testFairness() throws Exception {
        if (gridCount() == 1)
            return;

        // Total number of ops.
        final long opsCount = 10000;

        // Allowed deviation from uniform distribution.
        final double tolerance = 0.05;

        // Shared counter.
        final String OPS_COUNTER = "ops_counter";

        // Number of threads, one per node.
        final int threadCount = gridCount();

        final AtomicLong threadCounter = new AtomicLong(0);

        Ignite ignite = startGrid(gridCount());

        // Initialize reentrant lock.
        IgniteLock l = ignite.reentrantLock("lock", true, true, true);

        // Initialize OPS_COUNTER.
        ignite.getOrCreateCache(OPS_COUNTER).put(OPS_COUNTER, (long)0);

        final Map<Integer, Long> counts = new ConcurrentHashMap<>();

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    final int localNodeId = (int)threadCounter.getAndIncrement();

                    final Ignite grid = grid(localNodeId);

                    IgniteClosure<Ignite, Long> closure = new IgniteClosure<Ignite, Long>() {
                        @Override public Long apply(Ignite ignite) {
                            IgniteLock l = ignite.reentrantLock("lock", true, true, true);

                            long localCount = 0;

                            IgniteCountDownLatch latch = ignite.countDownLatch("latch", threadCount, false, true);

                            latch.countDown();

                            latch.await();

                            while(true){
                                l.lock();

                                try {
                                    long opsCounter = (long) ignite.getOrCreateCache(OPS_COUNTER).get(OPS_COUNTER);

                                    if(opsCounter == opsCount)
                                        break;

                                    ignite.getOrCreateCache(OPS_COUNTER).put(OPS_COUNTER, ++opsCounter);

                                    localCount++;

                                    if(localCount > 1000){
                                        assertTrue(localCount < (1 + tolerance) * opsCounter / threadCount);

                                        assertTrue(localCount > (1 - tolerance) * opsCounter / threadCount);
                                    }

                                    if(localCount % 100 == 0) {
                                        info("Node [id=" +ignite.cluster().localNode().id() + "] acquired " +
                                            localCount + " times. " + "Total ops count: " +
                                            opsCounter + "/" + opsCount +"]");
                                    }
                                }
                                finally {
                                    l.unlock();
                                }
                            }

                            return localCount;
                        }
                    };

                    long localCount = closure.apply(grid);

                    counts.put(localNodeId, localCount);

                    return null;
                }
            }, threadCount);

        fut.get();

        long totalSum = 0;

        for(int i=0; i<gridCount(); i++){

            totalSum += counts.get(i);

            info("Node " + grid(i).localNode().id() + " acquired the lock " + counts.get(i) + " times. ");
        }

        assertEquals(totalSum, opsCount);

        l.close();

        ignite.close();
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
