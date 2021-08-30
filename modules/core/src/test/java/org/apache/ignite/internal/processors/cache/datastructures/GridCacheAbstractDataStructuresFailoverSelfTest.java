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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.lang.Boolean.TRUE;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Failover tests for cache data structures.
 */
public abstract class GridCacheAbstractDataStructuresFailoverSelfTest extends IgniteCollectionAbstractTest {
    /** */
    private static final long TEST_TIMEOUT = 3 * 60 * 1000;

    /** */
    private static final String NEW_IGNITE_INSTANCE_NAME = "newGrid";

    /** */
    private static final String STRUCTURE_NAME = "structure";

    /** */
    private static final String TRANSACTIONAL_CACHE_NAME = "tx_cache";

    /** */
    private static final String CLIENT_INSTANCE_NAME = "client";

    /** */
    private static final int TOP_CHANGE_CNT = 2;

    /** */
    private static final int TOP_CHANGE_THREAD_CNT = 2;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * @return Grids count to start.
     */
    @Override public int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(gridCount());

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        atomicCfg.setCacheMode(collectionCacheMode());
        atomicCfg.setBackups(collectionConfiguration().getBackups());

        cfg.setAtomicConfiguration(atomicCfg);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(TRANSACTIONAL_CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        if (cfg.isClientMode() == TRUE)
            ((TcpDiscoverySpi)(cfg.getDiscoverySpi())).setForceServerMode(true);

        return cfg;
    }

    /**
     * Starts client node.
     *
     * @return client node.
     * @throws Exception If failed.
     */
    protected IgniteEx startClient() throws Exception {
        return startClientGrid(getConfiguration(CLIENT_INSTANCE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongFailsWhenServersLeft() throws Exception {
        Ignite ignite = startClientGrid(gridCount());

        new Timer().schedule(new TimerTask() {
            @Override public void run() {
                for (int i = 0; i < gridCount(); i++)
                    stopGrid(i);
            }
        }, 10_000);

        long stopTime = U.currentTimeMillis() + TEST_TIMEOUT / 2;

        IgniteAtomicLong atomic = ignite.atomicLong(STRUCTURE_NAME, 10, true);

        try {
            while (U.currentTimeMillis() < stopTime)
                assertEquals(10, atomic.get());
        }
        catch (IgniteException ignore) {
            return; // Test that client does not hang.
        }

        fail();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongTopologyChange() throws Exception {
        try (IgniteAtomicLong atomic = grid(0).atomicLong(STRUCTURE_NAME, 10, true)) {
            Ignite g = startGrid(NEW_IGNITE_INSTANCE_NAME);

            assertEquals(10, g.atomicLong(STRUCTURE_NAME, 10, false).get());

            assertEquals(20, g.atomicLong(STRUCTURE_NAME, 10, false).addAndGet(10));

            stopGrid(NEW_IGNITE_INSTANCE_NAME);

            assertEquals(20, grid(0).atomicLong(STRUCTURE_NAME, 10, true).get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongConstantTopologyChange() throws Exception {
        doTestAtomicLong(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicLongConstantMultipleTopologyChange() throws Exception {
        doTestAtomicLong(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT));
    }

    /**
     * Tests IgniteAtomicLong.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestAtomicLong(ConstantTopologyChangeWorker topWorker) throws Exception {
        try (IgniteAtomicLong s = grid(0).atomicLong(STRUCTURE_NAME, 1, true)) {
            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    assert ignite.atomicLong(STRUCTURE_NAME, 1, true).get() > 0;

                    return null;
                }
            });

            long val = s.get();

            while (!fut.isDone()) {
                assertEquals(val, s.get());

                assertEquals(++val, s.incrementAndGet());
            }

            fut.get();

            for (Ignite g : G.allGrids())
                assertEquals(val, g.atomicLong(STRUCTURE_NAME, 1, false).get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicReferenceTopologyChange() throws Exception {
        try (IgniteAtomicReference atomic = grid(0).atomicReference(STRUCTURE_NAME, 10, true)) {
            Ignite g = startGrid(NEW_IGNITE_INSTANCE_NAME);

            assertEquals((Integer)10, g.atomicReference(STRUCTURE_NAME, 10, false).get());

            g.atomicReference(STRUCTURE_NAME, 10, false).set(20);

            stopGrid(NEW_IGNITE_INSTANCE_NAME);

            assertEquals((Integer)20, grid(0).atomicReference(STRUCTURE_NAME, 10, true).get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicReferenceConstantTopologyChange() throws Exception {
        doTestAtomicReference(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicReferenceConstantMultipleTopologyChange() throws Exception {
        doTestAtomicReference(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT));
    }

    /**
     * Tests atomic reference.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestAtomicReference(ConstantTopologyChangeWorker topWorker) throws Exception {
        try (IgniteAtomicReference<Integer> s = grid(0).atomicReference(STRUCTURE_NAME, 1, true)) {
            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    assert ignite.atomicReference(STRUCTURE_NAME, 1, false).get() > 0;

                    return null;
                }
            });

            int val = s.get();

            while (!fut.isDone()) {
                assertEquals(val, (int)s.get());

                s.set(++val);
            }

            fut.get();

            for (Ignite g : G.allGrids())
                assertEquals(val, (int)g.atomicReference(STRUCTURE_NAME, 1, true).get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicStampedTopologyChange() throws Exception {
        try (IgniteAtomicStamped atomic = grid(0).atomicStamped(STRUCTURE_NAME, 10, 10, true)) {
            Ignite g = startGrid(NEW_IGNITE_INSTANCE_NAME);

            IgniteBiTuple<Integer, Integer> t = g.atomicStamped(STRUCTURE_NAME, 10, 10, false).get();

            assertEquals((Integer)10, t.get1());
            assertEquals((Integer)10, t.get2());

            g.atomicStamped(STRUCTURE_NAME, 10, 10, false).set(20, 20);

            stopGrid(NEW_IGNITE_INSTANCE_NAME);

            t = grid(0).atomicStamped(STRUCTURE_NAME, 10, 10, false).get();

            assertEquals((Integer)20, t.get1());
            assertEquals((Integer)20, t.get2());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicStampedConstantTopologyChange() throws Exception {
        doTestAtomicStamped(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicStampedConstantMultipleTopologyChange() throws Exception {
        doTestAtomicStamped(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT));
    }

    /**
     * Tests atomic stamped value.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestAtomicStamped(ConstantTopologyChangeWorker topWorker) throws Exception {
        try (IgniteAtomicStamped<Integer, Integer> s = grid(0).atomicStamped(STRUCTURE_NAME, 1, 1, true)) {
            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    IgniteBiTuple<Integer, Integer> t = ignite.atomicStamped(STRUCTURE_NAME, 1, 1, false).get();

                    assert t.get1() > 0;
                    assert t.get2() > 0;

                    return null;
                }
            });

            int val = s.value();

            while (!fut.isDone()) {
                IgniteBiTuple<Integer, Integer> t = s.get();

                assertEquals(val, (int)t.get1());
                assertEquals(val, (int)t.get2());

                ++val;

                s.set(val, val);
            }

            fut.get();

            for (Ignite g : G.allGrids()) {
                IgniteBiTuple<Integer, Integer> t = g.atomicStamped(STRUCTURE_NAME, 1, 1, false).get();

                assertEquals(val, (int)t.get1());
                assertEquals(val, (int)t.get2());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCountDownLatchTopologyChange() throws Exception {
        try (IgniteCountDownLatch latch = grid(0).countDownLatch(STRUCTURE_NAME, 20, true, true)) {
            try {
                Ignite g = startGrid(NEW_IGNITE_INSTANCE_NAME);

                assertEquals(20, g.countDownLatch(STRUCTURE_NAME, 20, true, false).count());

                g.countDownLatch(STRUCTURE_NAME, 20, true, false).countDown(10);

                stopGrid(NEW_IGNITE_INSTANCE_NAME);

                assertEquals(10, grid(0).countDownLatch(STRUCTURE_NAME, 20, true, false).count());
            }
            finally {
                grid(0).countDownLatch(STRUCTURE_NAME, 20, true, false).countDownAll();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSemaphoreFailoverSafe() throws Exception {
        try (final IgniteSemaphore semaphore = grid(0).semaphore(STRUCTURE_NAME, 20, true, true)) {
            Ignite g = startGrid(NEW_IGNITE_INSTANCE_NAME);

            IgniteSemaphore semaphore2 = g.semaphore(STRUCTURE_NAME, 20, true, false);

            assertEquals(20, semaphore2.availablePermits());

            semaphore2.acquire(10);

            stopGrid(NEW_IGNITE_INSTANCE_NAME);

            waitForCondition(new PA() {
                @Override public boolean apply() {
                    return semaphore.availablePermits() == 20;
                }
            }, 2000);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSemaphoreNonFailoverSafe() throws Exception {
        try (IgniteSemaphore sem = grid(0).semaphore(STRUCTURE_NAME, 20, false, true)) {
            Ignite g = startGrid(NEW_IGNITE_INSTANCE_NAME);

            IgniteSemaphore sem2 = g.semaphore(STRUCTURE_NAME, 20, false, false);

            sem2.acquire(20);

            assertEquals(0, sem.availablePermits());

            new Timer().schedule(new TimerTask() {
                @Override public void run() {
                    stopGrid(NEW_IGNITE_INSTANCE_NAME);
                }
            }, 2000);

            try {
                sem.acquire(1);
            }
            catch (IgniteInterruptedException ignored) {
                // Expected exception.
                return;
            }
        }

        fail("Thread hasn't been interrupted");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCanCloseSetInInterruptedThread() throws Exception {
        doCloseByInterruptedThread(grid(0).set(STRUCTURE_NAME, new CollectionConfiguration()));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCanCloseQueueInInterruptedThread() throws Exception {
        doCloseByInterruptedThread(grid(0).queue(STRUCTURE_NAME, 0, new CollectionConfiguration()));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCanCloseAtomicLongInInterruptedThread() throws Exception {
        doCloseByInterruptedThread(grid(0).atomicLong(STRUCTURE_NAME, 10, true));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCanCloseAtomicReferenceInInterruptedThread() throws Exception {
        doCloseByInterruptedThread(grid(0).atomicReference(STRUCTURE_NAME, 10, true));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCanCloseCountDownLatchInInterruptedThread() throws Exception {
        IgniteCountDownLatch latch = grid(0).countDownLatch(STRUCTURE_NAME, 1, true, true);
        latch.countDown();

        doCloseByInterruptedThread(latch);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCanCloseAtomicStampedInInterruptedThread() throws Exception {
        doCloseByInterruptedThread(grid(0).atomicStamped(STRUCTURE_NAME, 10, 10, true));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCanCloseSemaphoreInInterruptedThread() throws Exception {
        doCloseByInterruptedThread(grid(0).semaphore(STRUCTURE_NAME, 1, true, true));
    }

    /**
     * Tries close datastructure in interrupted thread
     *
     * @param closeableDs DataStructure to close.
     * @throws Exception If failed.
     */
    private void doCloseByInterruptedThread(final Closeable closeableDs) throws Exception {
        Thread.currentThread().interrupt();

        try {
            closeableDs.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            Thread.interrupted();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSemaphoreSingleNodeFailure() throws Exception {
        final Ignite i1 = grid(0);

        IgniteSemaphore sem1 = i1.semaphore(STRUCTURE_NAME, 1, false, true);

        sem1.acquire();

        final CountDownLatch createLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() {
                boolean failed = true;

                IgniteSemaphore sem2 = i1.semaphore(STRUCTURE_NAME, 1, false, true);

                try {
                    // Guard the acquire call by count down latch to make sure that semaphore creation does not fail.
                    createLatch.countDown();

                    sem2.acquire();
                }
                catch (Exception ignored) {
                    failed = false;
                }
                finally {
                    assertFalse(failed);

                    sem2.release();
                }
                return null;
            }
        });

        assertTrue("Failed to wait for semaphore creation",
            createLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        while (!sem1.hasQueuedThreads()) {
            try {
                Thread.sleep(1);
            }
            catch (InterruptedException ignored) {
                fail();
            }
        }

        i1.close();

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSemaphoreConstantTopologyChangeFailoverSafe() throws Exception {
        doTestSemaphore(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSemaphoreConstantTopologyChangeNonFailoverSafe() throws Exception {
        doTestSemaphore(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSemaphoreMultipleTopologyChangeFailoverSafe() throws Exception {
        doTestSemaphore(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSemaphoreMultipleTopologyChangeNonFailoverSafe() throws Exception {
        doTestSemaphore(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), false);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestSemaphore(ConstantTopologyChangeWorker topWorker, final boolean failoverSafe) throws Exception {
        final int permits = topWorker instanceof MultipleTopologyChangeWorker ||
            topWorker instanceof PartitionedMultipleTopologyChangeWorker ? TOP_CHANGE_THREAD_CNT * 3 :
            TOP_CHANGE_CNT;

        try (IgniteSemaphore s = grid(0).semaphore(STRUCTURE_NAME, permits, failoverSafe, true)) {
            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    IgniteSemaphore sem = ignite.semaphore(STRUCTURE_NAME, permits, failoverSafe, false);

                    while (true) {
                        try {
                            sem.acquire(1);

                            break;
                        }
                        catch (IgniteInterruptedException e) {
                           // Exception may happen in non failover safe mode.
                            if (failoverSafe)
                                throw e;
                            else {
                                // In non-failoverSafe mode semaphore is not safe to be reused,
                                // and should always be discarded after exception is caught.
                                break;
                            }
                        }
                    }

                    return null;
                }
            });

            while (!fut.isDone()) {
                while (true) {
                    try {
                        s.acquire(1);

                        break;
                    }
                    catch (IgniteInterruptedException e) {
                        // Exception may happen in non failover safe mode.
                        if (failoverSafe)
                            throw e;
                        else {
                            // In non-failoverSafe mode semaphore is not safe to be reused,
                            // and should always be discarded after exception is caught.
                            break;
                        }
                    }
                }

                assert s.availablePermits() < permits;

                s.release();

                assert s.availablePermits() <= permits;
            }

            fut.get();

            // Semaphore is left in proper state only if failoverSafe mode is used.
            if (failoverSafe) {
                for (Ignite g : G.allGrids())
                    assertEquals(permits, g.semaphore(STRUCTURE_NAME, permits, false, false).availablePermits());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReentrantLockFailsWhenServersLeft() throws Exception {
        testReentrantLockFailsWhenServersLeft(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFairReentrantLockFailsWhenServersLeft() throws Exception {
        testReentrantLockFailsWhenServersLeft(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockFailsWhenServersLeft(final boolean fair) throws Exception {
        Ignite client = startClientGrid(gridCount());

        Ignite server = grid(0);

        // Initialize lock.
        IgniteLock srvLock = server.reentrantLock("lock", true, fair, true);

        IgniteSemaphore semaphore = server.semaphore("sync", 0, true, true);

        IgniteFuture fut = client.compute().applyAsync(new IgniteClosure<Ignite, Object>() {
            @Override public Object apply(Ignite ignite) {
                final IgniteLock l = ignite.reentrantLock("lock", true, fair, true);

                l.lock();

                assertTrue(l.isHeldByCurrentThread());

                l.unlock();

                assertFalse(l.isHeldByCurrentThread());

                // Signal the server to go down.
                ignite.semaphore("sync", 0, true, true).release();

                boolean isExceptionThrown = false;

                try {
                    // Wait for the server to go down.
                    Thread.sleep(1000);

                    l.lock();

                    fail("Exception must be thrown.");
                }
                catch (InterruptedException ignored) {
                    fail("Interrupted exception not expected here.");
                }
                catch (IgniteException ignored) {
                    isExceptionThrown = true;
                }
                finally {
                    assertTrue(isExceptionThrown);

                    assertFalse(l.isHeldByCurrentThread());
                }
                return null;
            }
        }, client);

        // Wait for the lock on client to be acquired then released.
        semaphore.acquire();

        for (int i = 0; i < gridCount(); i++)
            stopGrid(i);

        fut.get();

        client.close();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReentrantLockConstantTopologyChangeFailoverSafe() throws Exception {
        doTestReentrantLock(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReentrantLockConstantMultipleTopologyChangeFailoverSafe() throws Exception {
        doTestReentrantLock(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReentrantLockConstantTopologyChangeNonFailoverSafe() throws Exception {
        doTestReentrantLock(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReentrantLockConstantMultipleTopologyChangeNonFailoverSafe() throws Exception {
        doTestReentrantLock(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFairReentrantLockConstantTopologyChangeFailoverSafe() throws Exception {
        doTestReentrantLock(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFairReentrantLockConstantMultipleTopologyChangeFailoverSafe() throws Exception {
        doTestReentrantLock(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFairReentrantLockConstantTopologyChangeNonFailoverSafe() throws Exception {
        doTestReentrantLock(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFairReentrantLockConstantMultipleTopologyChangeNonFailoverSafe() throws Exception {
        doTestReentrantLock(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT), false, true);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestReentrantLock(
        final ConstantTopologyChangeWorker topWorker,
        final boolean failoverSafe,
        final boolean fair
    ) throws Exception {
        IgniteEx ig = grid(0);

        try (IgniteLock lock = ig.reentrantLock(STRUCTURE_NAME, failoverSafe, fair, true)) {
            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Void>() {
                @Override public Void apply(Ignite ignite) {
                    final IgniteLock l = ignite.reentrantLock(STRUCTURE_NAME, failoverSafe, fair, false);

                    final AtomicBoolean done = new AtomicBoolean(false);

                    GridTestUtils.runAsync(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            try {
                                l.lock();
                            }
                            finally {
                                done.set(true);
                            }

                            return null;
                        }
                    }, "lock-thread");

                    // Wait until l.lock() has been called.
                    while (!l.hasQueuedThreads() && !done.get()){
                        // No-op.
                    }

                    return null;
                }
            });

            long endTime = System.currentTimeMillis() + getTestTimeout();

            while (!fut.isDone()) {
                try {
                    lock.lock();
                }
                catch (IgniteException e) {
                    // Exception may happen in non-failoversafe mode.
                    if (failoverSafe)
                        throw e;
                    // problem already occurred, test is being shutdown
                    if (Thread.currentThread().isInterrupted())
                        throw e;
                }
                finally {
                    // Broken lock cannot be used in non-failoversafe mode.
                    if (!lock.isBroken() || failoverSafe) {
                        assertTrue(lock.isHeldByCurrentThread());

                        lock.unlock();

                        assertFalse(lock.isHeldByCurrentThread());
                    }
                }

                if (System.currentTimeMillis() > endTime)
                    fail("Failed to wait for topology change threads.");
            }

            fut.get();

            for (Ignite g : G.allGrids()) {
                IgniteLock l = g.reentrantLock(STRUCTURE_NAME, failoverSafe, fair, false);

                assertTrue(g.name(), !l.isHeldByCurrentThread() || lock.isBroken());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCountDownLatchConstantTopologyChange() throws Exception {
        doTestCountDownLatch(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCountDownLatchConstantMultipleTopologyChange() throws Exception {
        doTestCountDownLatch(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT));
    }

    /**
     * Tests distributed count down latch.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestCountDownLatch(ConstantTopologyChangeWorker topWorker) throws Exception {
        try (IgniteCountDownLatch s = grid(0).countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, true)) {
            try {
                IgniteInternalFuture<?> fut = topWorker.startChangingTopology(
                    new IgniteClosure<Ignite, Object>() {
                        @Override public Object apply(Ignite ignite) {
                            assert ignite.countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, false).count() > 0;

                            return null;
                        }
                    });

                int val = s.count();

                while (!fut.isDone()) {
                    assertEquals(val, s.count());
                    assertEquals(--val, s.countDown());
                }

                fut.get();

                for (Ignite g : G.allGrids())
                    assertEquals(val, g.countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, true).count());
            }
            finally {
                grid(0).countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, false).countDownAll();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFifoQueueTopologyChange() throws Exception {
        try {
            grid(0).queue(STRUCTURE_NAME, 0, config(false)).put(10);

            Ignite g = startGrid(NEW_IGNITE_INSTANCE_NAME);

            assertEquals(10, (int)g.<Integer>queue(STRUCTURE_NAME, 0, null).poll());

            g.queue(STRUCTURE_NAME, 0, null).put(20);

            stopGrid(NEW_IGNITE_INSTANCE_NAME);

            assertEquals(20, (int)grid(0).<Integer>queue(STRUCTURE_NAME, 0, null).peek());
        }
        finally {
            grid(0).<Integer>queue(STRUCTURE_NAME, 0, null).close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueTopologyChange() throws Exception {
        ConstantTopologyChangeWorker topWorker = new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT);

        try (final IgniteQueue<Integer> q = grid(0).queue(STRUCTURE_NAME, 0, config(false))) {
            for (int i = 0; i < 1000; i++)
                q.add(i);

            final IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    return null;
                }
            });

            IgniteInternalFuture<?> takeFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!fut.isDone())
                        q.take();

                    return null;
                }
            });

            IgniteInternalFuture<?> pollFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!fut.isDone())
                        q.poll();

                    return null;
                }
            });

            IgniteInternalFuture<?> addFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!fut.isDone())
                        q.add(0);

                    return null;
                }
            });

            fut.get();

            pollFut.get();
            addFut.get();

            q.add(0);

            takeFut.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueConstantTopologyChange() throws Exception {
        int topChangeThreads = collectionCacheMode() == CacheMode.PARTITIONED ? 1 : TOP_CHANGE_THREAD_CNT;

        doTestQueue(new ConstantTopologyChangeWorker(topChangeThreads));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueConstantMultipleTopologyChange() throws Exception {
        int topChangeThreads = collectionCacheMode() == CacheMode.PARTITIONED ? 1 : TOP_CHANGE_THREAD_CNT;

        doTestQueue(multipleTopologyChangeWorker(topChangeThreads));
    }

    /**
     * Tests the queue.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestQueue(ConstantTopologyChangeWorker topWorker) throws Exception {
        int queueMaxSize = 100;

        try (IgniteQueue<Integer> s = grid(0).queue(STRUCTURE_NAME, 0, config(false))) {
            s.put(1);

            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    IgniteQueue<Integer> queue = ignite.queue(STRUCTURE_NAME, 0, null);

                    assertNotNull(queue);

                    Integer val = queue.peek();

                    assertNotNull(val);

                    assert val > 0;

                    return null;
                }
            });

            int val = s.peek();

            while (!fut.isDone()) {
                if (s.size() == queueMaxSize) {
                    int last = 0;

                    for (int i = 0, size = s.size() - 1; i < size; i++) {
                        int cur = s.poll();

                        if (i == 0) {
                            last = cur;

                            continue;
                        }

                        assertEquals(last, cur - 1);

                        last = cur;
                    }
                }

                s.put(++val);
            }

            fut.get();

            val = s.peek();

            for (Ignite g : G.allGrids())
                assertEquals(val, (int)g.<Integer>queue(STRUCTURE_NAME, 0, null).peek());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicSequenceInitialization() throws Exception {
        checkAtomicSequenceInitialization(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicSequenceInitializationOnStableNodes() throws Exception {
        checkAtomicSequenceInitialization(true);
    }

    /**
     * @param limitProjection {@code True} if test should call init only on stable nodes.
     * @throws Exception If failed.
     */
    private void checkAtomicSequenceInitialization(boolean limitProjection) throws Exception {
        int threadCnt = 3;

        IgniteCompute compute;

        if (limitProjection) {
            List<UUID> nodeIds = new ArrayList<>(gridCount());

            for (int i = 0; i < gridCount(); i++)
                nodeIds.add(grid(i).cluster().localNode().id());

            compute = grid(0).compute(grid(0).cluster().forNodeIds(nodeIds));
        }
        else
            compute = grid(0).compute();

        final AtomicInteger idx = new AtomicInteger(gridCount());

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
            @Override public void apply() {
                int id = idx.getAndIncrement();

                try {
                    log.info("Start node: " + id);

                    startGrid(id);

                    Thread.sleep(1000);

                }
                catch (Exception e) {
                    throw F.wrap(e);
                }
                finally {
                    stopGrid(id);

                    info("Thread finished.");
                }
            }
        }, threadCnt, "test-thread");

        while (!fut.isDone()) {
            compute.call(new IgniteCallable<Object>() {
                /** */
                @IgniteInstanceResource
                private Ignite g;

                @Override public Object call() {
                    try {
                        IgniteAtomicSequence seq = g.atomicSequence(STRUCTURE_NAME, 1, true);

                        assert seq != null;

                        for (int i = 0; i < 1000; i++)
                            seq.getAndIncrement();

                        return null;
                    }
                    catch (IgniteException e) {
                        // Fail if we are on stable nodes or exception is not node stop.
                        if (limitProjection || !X.hasCause(e, NodeStoppingException.class))
                            throw e;

                        return null;
                    }
                }
            });
        }

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicSequenceTopologyChange() throws Exception {
        try (IgniteAtomicSequence s = grid(0).atomicSequence(STRUCTURE_NAME, 10, true)) {
            Ignite g = startGrid(NEW_IGNITE_INSTANCE_NAME);

            assertEquals(1010, g.atomicSequence(STRUCTURE_NAME, 10, false).get());

            assertEquals(1020, g.atomicSequence(STRUCTURE_NAME, 10, false).addAndGet(10));

            stopGrid(NEW_IGNITE_INSTANCE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicSequenceConstantTopologyChange() throws Exception {
        doTestAtomicSequence(new ConstantTopologyChangeWorker(TOP_CHANGE_THREAD_CNT, true));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicSequenceConstantMultipleTopologyChange() throws Exception {
        doTestAtomicSequence(multipleTopologyChangeWorker(TOP_CHANGE_THREAD_CNT));
    }

    /**
     * Tests atomic sequence.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestAtomicSequence(ConstantTopologyChangeWorker topWorker) throws Exception {
        try (IgniteAtomicSequence s = startClient().atomicSequence(STRUCTURE_NAME, 1, true)) {
            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    assertTrue(ignite.atomicSequence(STRUCTURE_NAME, 1, false).get() > 0);

                    return null;
                }
            });

            long old = s.get();

            while (!fut.isDone()) {
                assertEquals(old, s.get());

                long val = s.incrementAndGet();

                assertTrue(val > old);

                old = val;
            }

            fut.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUncommitedTxLeave() throws Exception {
        final int val = 10;

        grid(0).atomicLong(STRUCTURE_NAME, val, true);

        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Ignite g = startGrid(NEW_IGNITE_INSTANCE_NAME);

                try {
                    g.transactions().txStart();

                    g.cache(TRANSACTIONAL_CACHE_NAME).put(1, 1);

                    assertEquals(val + 1, g.atomicLong(STRUCTURE_NAME, val, false).incrementAndGet());
                }
                finally {
                    stopGrid(NEW_IGNITE_INSTANCE_NAME);
                }

                return null;
            }
        }).get();

        waitForDiscovery(G.allGrids().toArray(new Ignite[gridCount()]));

        assertEquals(val + 1, grid(0).atomicLong(STRUCTURE_NAME, val, false).get());
    }

    /**
     * @param topChangeThreads Number of topology change threads.
     *
     * @return Specific multiple topology change worker implementation.
     */
    private ConstantTopologyChangeWorker multipleTopologyChangeWorker(int topChangeThreads) {
        return collectionCacheMode() == CacheMode.PARTITIONED ?
            new PartitionedMultipleTopologyChangeWorker(topChangeThreads) :
            new MultipleTopologyChangeWorker(topChangeThreads);
    }

    /**
     *
     */
    private class ConstantTopologyChangeWorker {
        /** */
        protected final AtomicBoolean failed = new AtomicBoolean(false);

        /** */
        protected final int topChangeThreads;

        /** Flag to enable circular topology change. */
        private boolean circular;

        /**
         * @param topChangeThreads Number of topology change threads.
         */
        public ConstantTopologyChangeWorker(int topChangeThreads) {
            this.topChangeThreads = topChangeThreads;
        }

        /**
         * @param topChangeThreads Number of topology change threads.
         * @param circular flag to enable circular topology change.
         */
        public ConstantTopologyChangeWorker(int topChangeThreads, boolean circular) {
            this.topChangeThreads = topChangeThreads;
            this.circular = circular;
        }

        /**
         * Starts changing cluster's topology.
         *
         * @param cb Callback to run after node start.
         * @return Future.
         */
        IgniteInternalFuture<?> startChangingTopology(final IgniteClosure<Ignite, ?> cb) {
            final AtomicInteger nodeIdx = new AtomicInteger(G.allGrids().size());

            return GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            if (failed.get())
                                return;

                            int idx = nodeIdx.incrementAndGet();

                            Thread.currentThread().setName("thread-" + getTestIgniteInstanceName(idx));

                            try {
                                log.info("Start node: " + getTestIgniteInstanceName(idx));

                                Ignite g = startGrid(idx);

                                cb.apply(g);
                            }
                            catch (IgniteException e) {
                                if (!X.hasCause(e, NodeStoppingException.class) &&
                                    !X.hasCause(e, IllegalStateException.class))
                                    throw e;

                                // OK for this test.
                            }
                            finally {
                                if (circular)
                                    stopGrid(G.allGrids().get(0).configuration().getIgniteInstanceName());
                                else
                                    stopGrid(idx);
                            }
                        }
                    }
                    catch (Exception e) {
                        if (failed.compareAndSet(false, true))
                            throw F.wrap(e);
                    }
                }
            }, topChangeThreads, "topology-change-thread");
        }
    }

    /**
     *
     */
    private class MultipleTopologyChangeWorker extends ConstantTopologyChangeWorker {
        /**
         * @param topChangeThreads Number of topology change threads.
         */
        public MultipleTopologyChangeWorker(int topChangeThreads) {
            super(topChangeThreads);
        }

        /**
         * Starts changing cluster's topology.
         *
         * @return Future.
         */
        @Override IgniteInternalFuture<?> startChangingTopology(final IgniteClosure<Ignite, ?> cb) {
            return GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            if (failed.get())
                                return;

                            Collection<String> names = new GridLeanSet<>(3);

                            try {
                                for (int j = 0; j < 3; j++) {
                                    if (failed.get())
                                        return;

                                    String name = UUID.randomUUID().toString();

                                    log.info("Start node: " + name);

                                    Ignite g = startGrid(name);

                                    names.add(name);

                                    cb.apply(g);
                                }
                            }
                            finally {
                                for (String name : names)
                                    stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        if (failed.compareAndSet(false, true))
                            throw F.wrap(e);
                    }
                }
            }, topChangeThreads, "topology-change-thread");
        }
    }

    /**
     *
     */
    private class PartitionedMultipleTopologyChangeWorker extends ConstantTopologyChangeWorker {
        /** */
        private CyclicBarrier barrier;

        /**
         * @param topChangeThreads Number of topology change threads.
         */
        public PartitionedMultipleTopologyChangeWorker(int topChangeThreads) {
            super(topChangeThreads);
        }

        /**
         * Starts changing cluster's topology.
         *
         * @return Future.
         */
        @Override IgniteInternalFuture<?> startChangingTopology(final IgniteClosure<Ignite, ?> cb) {
            final Semaphore sem = new Semaphore(TOP_CHANGE_THREAD_CNT);

            final ConcurrentSkipListSet<String> startedNodes = new ConcurrentSkipListSet<>();

            barrier = new CyclicBarrier(TOP_CHANGE_THREAD_CNT, new Runnable() {
                @Override public void run() {
                    try {
                        assertEquals(TOP_CHANGE_THREAD_CNT * 3, startedNodes.size());

                        for (String name : startedNodes) {
                            stopGrid(name, false);

                            awaitPartitionMapExchange();
                        }

                        startedNodes.clear();

                        sem.release(TOP_CHANGE_THREAD_CNT);

                        barrier.reset();
                    }
                    catch (Exception e) {
                        if (failed.compareAndSet(false, true)) {
                            sem.release(TOP_CHANGE_THREAD_CNT);

                            barrier.reset();

                            throw F.wrap(e);
                        }
                    }
                }
            });

            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            sem.acquire();

                            if (failed.get())
                                return;

                            for (int j = 0; j < 3; j++) {
                                if (failed.get())
                                    return;

                                String name = UUID.randomUUID().toString();

                                startedNodes.add(name);

                                log.info("Start node: " + name);

                                Ignite g = startGrid(name);

                                cb.apply(g);
                            }

                            try {
                                barrier.await();
                            }
                            catch (BrokenBarrierException ignored) {
                                // No-op.
                            }
                        }
                    }
                    catch (Exception e) {
                        if (failed.compareAndSet(false, true)) {
                            sem.release(TOP_CHANGE_THREAD_CNT);

                            barrier.reset();

                            throw F.wrap(e);
                        }
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            return fut;
        }
    }
}
