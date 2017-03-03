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
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridStringLogger;
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
 * Cache semaphore self test.
 */
public abstract class IgniteSemaphoreAbstractSelfTest extends IgniteAtomicsAbstractTest
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
    public void testSemaphore() throws Exception {
        checkSemaphore();
        checkSemaphoreSerialization();
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
            IgniteSemaphore semaphore = ignite.semaphore("testIsolation", 1, true, true);

            assertNotNull(semaphore);

            try (Transaction tx = ignite.transactions().txStart()) {
                cache.put(1, 1);

                assertEquals(1, semaphore.availablePermits());

                semaphore.acquire();

                tx.rollback();
            }

            assertEquals(0, cache.size());

            assertEquals(0, semaphore.availablePermits());

            semaphore.close();

            assertTrue(semaphore.removed());
        }
        finally {
            ignite.destroyCache(cfg.getName());
        }
    }

    /**
     * @param failoverSafe Failover safe flag.
     * @throws Exception If failed.
     */
    private void checkFailover(boolean failoverSafe) throws Exception {
        IgniteEx g = startGrid(NODES_CNT + 1);

        // For vars locality.
        {
            // Ensure not exists.
            assert g.semaphore("sem", 2, failoverSafe, false) == null;

            IgniteSemaphore sem = g.semaphore(
                "sem",
                2,
                failoverSafe,
                true);

            sem.acquire(2);

            assert !sem.tryAcquire();
            assertEquals(
                0,
                sem.availablePermits());
        }

        Ignite g0 = grid(0);

        final IgniteSemaphore sem0 = g0.semaphore(
            "sem",
            -10,
            false,
            false);

        assert !sem0.tryAcquire();
        assertEquals(0, sem0.availablePermits());

        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    sem0.acquire();

                    info("Acquired in separate thread.");

                    return null;
                }
            },
            1);

        Thread.sleep(100);

        g.close();

        try {
            fut.get(500);
        }
        catch (IgniteCheckedException e) {
            if (!failoverSafe && e.hasCause(InterruptedException.class))
                info("Ignored expected exception: " + e);
            else
                throw e;
        }

        sem0.close();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSemaphore() throws Exception {
        // Test API.
        checkAcquire();

        checkRelease();

        checkFailoverSafe();

        // Test main functionality.
        IgniteSemaphore semaphore1 = grid(0).semaphore("semaphore", -2, true, true);

        assertEquals(-2, semaphore1.availablePermits());

        IgniteCompute comp = grid(0).compute().withAsync();

        comp.call(new IgniteCallable<Object>() {
            @IgniteInstanceResource
            private Ignite ignite;

            @LoggerResource
            private IgniteLogger log;

            @Nullable @Override public Object call() throws Exception {
                // Test semaphore in multiple threads on each node.
                IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
                    new Callable<Object>() {
                        @Nullable @Override public Object call() throws Exception {
                            IgniteSemaphore semaphore = ignite.semaphore("semaphore", -2, true, true);

                            assert semaphore != null && semaphore.availablePermits() == -2;

                            log.info("Thread is going to wait on semaphore: " + Thread.currentThread().getName());

                            assert semaphore.tryAcquire(1, 1, MINUTES);

                            log.info("Thread is again runnable: " + Thread.currentThread().getName());

                            semaphore.release();

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

        semaphore1.release(2);

        assert semaphore1.availablePermits() == 0;

        semaphore1.release(1);

        // Ensure there are no hangs.
        fut.get();

        // Test operations on removed semaphore.
        semaphore1.close();

        checkRemovedSemaphore(semaphore1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSemaphoreClosing() throws Exception {
        IgniteConfiguration cfg;
        GridStringLogger stringLogger;

        stringLogger = new GridStringLogger();

        cfg = optimize(getConfiguration("npeGrid"));
        cfg.setGridLogger(stringLogger);

        try (Ignite ignite = startGrid(cfg.getGridName(), cfg)) {
            ignite.semaphore("semaphore", 1, true, true);
        }

        assertFalse(stringLogger.toString().contains(NullPointerException.class.getName()));
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSemaphoreSerialization() throws Exception {
        final IgniteSemaphore sem = grid(0).semaphore("semaphore", -gridCount() + 1, true, true);

        assertEquals(-gridCount() + 1, sem.availablePermits());

        grid(0).compute().broadcast(new IgniteCallable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                sem.release();

                return null;
            }
        });

        assert sem.availablePermits() == 1;

        sem.acquire();

        assert sem.availablePermits() == 0;

        sem.release();

        // Test operations on removed semaphore.
        sem.close();

        checkRemovedSemaphore(sem);
    }

    /**
     * @param semaphore Semaphore.
     * @throws Exception If failed.
     */
    protected void checkRemovedSemaphore(final IgniteSemaphore semaphore) throws Exception {
        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return semaphore.removed();
            }
        }, 5000);

        assert semaphore.removed();
    }

    /**
     * This method only checks if parameter of new semaphore is initialized properly. For tests considering failure
     * recovery see
     *
     * @throws Exception Exception.
     */
    private void checkFailoverSafe() throws Exception {
        // Checks only if semaphore is initialized properly
        IgniteSemaphore semaphore = createSemaphore("rmv", 5, true);

        assert semaphore.isFailoverSafe();

        removeSemaphore("rmv");

        IgniteSemaphore semaphore1 = createSemaphore("rmv1", 5, false);

        assert !semaphore1.isFailoverSafe();

        removeSemaphore("rmv1");
    }

    /**
     * @throws Exception Exception.
     */
    private void checkAcquire() throws Exception {
        // Check only 'false' cases here. Successful await is tested over the grid.
        IgniteSemaphore semaphore = createSemaphore("acquire", 5, false);

        assert !semaphore.tryAcquire(10);
        assert !semaphore.tryAcquire(10, 10, MICROSECONDS);

        removeSemaphore("acquire");
    }

    /**
     * @throws Exception Exception.
     */
    private void checkRelease() throws Exception {
        IgniteSemaphore semaphore = createSemaphore("release", 5, false);

        semaphore.release();
        assert semaphore.availablePermits() == 6;

        semaphore.release(2);
        assert semaphore.availablePermits() == 8;

        assert semaphore.drainPermits() == 8;
        assert semaphore.availablePermits() == 0;

        removeSemaphore("release");

        checkRemovedSemaphore(semaphore);

        IgniteSemaphore semaphore2 = createSemaphore("release2", -5, false);

        semaphore2.release();

        assert semaphore2.availablePermits() == -4;

        semaphore2.release(2);

        assert semaphore2.availablePermits() == -2;

        assert semaphore2.drainPermits() == -2;

        assert semaphore2.availablePermits() == 0;

        removeSemaphore("release2");

        checkRemovedSemaphore(semaphore2);
    }

    /**
     * @param semaphoreName Semaphore name.
     * @param numPermissions Initial number of permissions.
     * @param failoverSafe Fairness flag.
     * @return New semaphore.
     * @throws Exception If failed.
     */
    private IgniteSemaphore createSemaphore(String semaphoreName, int numPermissions, boolean failoverSafe)
        throws Exception {
        IgniteSemaphore semaphore = grid(RND.nextInt(NODES_CNT)).semaphore(semaphoreName, numPermissions, failoverSafe, true);

        // Test initialization.
        assert semaphoreName.equals(semaphore.name());
        assert semaphore.availablePermits() == numPermissions;
        assert semaphore.getQueueLength() == 0;
        assert semaphore.isFailoverSafe() == failoverSafe;

        return semaphore;
    }

    /**
     * @param semaphoreName Semaphore name.
     * @throws Exception If failed.
     */
    private void removeSemaphore(String semaphoreName)
        throws Exception {
        IgniteSemaphore semaphore = grid(RND.nextInt(NODES_CNT)).semaphore(semaphoreName, 10, false, true);

        assert semaphore != null;

        if (semaphore.availablePermits() < 0)
            semaphore.release(-semaphore.availablePermits());

        // Remove semaphore on random node.
        IgniteSemaphore semaphore0 = grid(RND.nextInt(NODES_CNT)).semaphore(semaphoreName, 0, false, true);

        assertNotNull(semaphore0);

        semaphore0.close();

        // Ensure semaphore is removed on all nodes.
        for (Ignite g : G.allGrids())
            assertNull(((IgniteKernal)g).context().dataStructures().semaphore(semaphoreName, 10, true, false));

        checkRemovedSemaphore(semaphore);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSemaphoreMultinode1() throws Exception {
        if (gridCount() == 1)
            return;

        IgniteSemaphore semaphore = grid(0).semaphore("s1", 0, true, true);

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < gridCount(); i++) {
            final Ignite ignite = grid(i);

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteSemaphore semaphore = ignite.semaphore("s1", 0, true, false);

                    assertNotNull(semaphore);

                    boolean wait = semaphore.tryAcquire(30_000, MILLISECONDS);

                    assertTrue(wait);

                    return null;
                }
            }));
        }

        for (int i = 0; i < 10; i++)
            semaphore.release();

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(30_000);
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
