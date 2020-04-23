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

package org.apache.ignite.internal.processors.compute;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Test that compute task will be rejected when executed within transaction or lock
 * if topology pending updates to prevent deadlock.
 */
@SuppressWarnings("ThrowableNotThrown")
public class IgniteComputeWithinLockAndPendingTopologyTest extends GridCommonAbstractTest {
    /** Maximum timeout for topology update (prevent hanging and long shutdown). */
    private static final long PENDING_TIMEOUT = 15_000L;

    /** */
    private static final long CACHE_CREATION_TIMEOUT = 2_000L;

    /** */
    private static final int CACHE_SIZE = 10;

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String CACHE3 = "cache3";

    /** */
    private static final String EXPECTED_ERROR_MESSAGE = "Pending " +
        "topology found - job execution within lock or transaction was canceled.";

    /** {@inheritDoc} */
    @Override public void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override public void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Create cache configuration with transactional atomicity mode.
     *
     * @param name Cache name.
     * @param mode {@link CacheMode Cache mode.}
     * @param writeMode {@link CacheWriteSynchronizationMode Cache write synchronization mode.}
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(String name, CacheMode mode,
        CacheWriteSynchronizationMode writeMode) {
        CacheConfiguration<Integer, Integer> cacheConfiguration = new CacheConfiguration<>(name);

        cacheConfiguration.setCacheMode(mode);
        cacheConfiguration.setWriteSynchronizationMode(writeMode);
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        return cacheConfiguration;
    }

    /**
     * Explicit lock test in {@link CacheMode#PARTITIONED partitioned} cache mode.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testExplicitLockPartitioned() throws Exception {
        for (final CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doTestExplicitLock(CacheMode.PARTITIONED, syncMode);

                    return null;
                }
            }, ComputeExecutionRejectedException.class, EXPECTED_ERROR_MESSAGE);
        }
    }

    /**
     * Explicit lock test in {@link CacheMode#REPLICATED replicated} cache mode.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testExplicitLockReplicated() throws Exception {
        for (final CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            assertThrowsAnyCause(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    doTestExplicitLock(CacheMode.REPLICATED, syncMode);

                    return null;
                }
            }, ComputeExecutionRejectedException.class, EXPECTED_ERROR_MESSAGE);
        }
    }

    /**
     * Explicit lock test in {@link CacheMode#LOCAL local} cache mode.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testExplicitLockLocal() throws Exception {
        for (final CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values())
            doTestExplicitLock(CacheMode.LOCAL, syncMode);
    }

    /**
     * Transactional lock test with {@link Transaction pessimistic}
     * concurrency control and {@link CacheMode#LOCAL local} cache mode.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testTransactionLockPessimisticLocal() throws Exception {
        for (final CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            for (final TransactionIsolation isolation : TransactionIsolation.values())
                doTestTx(CacheMode.LOCAL, syncMode, TransactionConcurrency.PESSIMISTIC, isolation);
        }
    }

    /**
     * Transactional lock test with {@link Transaction pessimistic}
     * concurrency control and {@link CacheMode#REPLICATED replicated} cache mode.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testTransactionLockPessimisticReplicated() throws Exception {
        for (final CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            for (final TransactionIsolation isolation : TransactionIsolation.values()) {
                assertThrowsAnyCause(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        doTestTx(CacheMode.REPLICATED, syncMode, TransactionConcurrency.PESSIMISTIC, isolation);

                        return null;
                    }
                }, ComputeExecutionRejectedException.class, EXPECTED_ERROR_MESSAGE);
            }
        }
    }

    /**
     * Transactional lock test with {@link Transaction pessimistic}
     * concurrency control and {@link CacheMode#PARTITIONED partitioned} cache mode.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testTransactionLockPessimisticPartitioned() throws Exception {
        for (final CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            for (final TransactionIsolation isolation : TransactionIsolation.values()) {
                assertThrowsAnyCause(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        doTestTx(CacheMode.PARTITIONED, syncMode, TransactionConcurrency.PESSIMISTIC, isolation);

                        return null;
                    }
                }, ComputeExecutionRejectedException.class, EXPECTED_ERROR_MESSAGE);
            }
        }
    }

    /**
     * Transactional lock test with {@link Transaction optimistic}
     * concurrency control and {@link CacheMode#REPLICATED replicated} cache mode.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testTransactionLockOptimisticReplicated() throws Exception {
        for (final CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            for (final TransactionIsolation isolation : TransactionIsolation.values())
                doTestTx(CacheMode.REPLICATED, syncMode, TransactionConcurrency.OPTIMISTIC, isolation);
        }
    }

    /**
     * Transactional lock test with {@link Transaction optimistic}
     * concurrency control and {@link CacheMode#PARTITIONED partitioned} cache mode.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testTransactionLockOptimisticPartitioned() throws Exception {
        for (final CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            for (final TransactionIsolation isolation : TransactionIsolation.values())
                doTestTx(CacheMode.PARTITIONED, syncMode, TransactionConcurrency.OPTIMISTIC, isolation);
        }
    }

    /**
     * Transactional lock test with {@link TransactionConcurrency#OPTIMISTIC optimistic}
     * concurrency control and {@link CacheMode#LOCAL local} cache mode.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testTransactionLockOptimisticLocal() throws Exception {
        for (final CacheWriteSynchronizationMode syncMode : CacheWriteSynchronizationMode.values()) {
            for (final TransactionIsolation isolation : TransactionIsolation.values())
                doTestTx(CacheMode.LOCAL, syncMode, TransactionConcurrency.OPTIMISTIC, isolation);
        }
    }

    /**
     * Test execution within lock.
     *
     * @param mode {@link CacheMode Cache mode.}
     * @param writeMode {@link CacheWriteSynchronizationMode Cache write synchronization mode.}
     * @throws CacheException If compute task execution was rejected.
     */
    private void doTestExplicitLock(CacheMode mode, CacheWriteSynchronizationMode writeMode) throws Exception {
        final Ignite node = grid(0);

        final GridCachePartitionExchangeManager exchange = grid(0).context().cache().context().exchange();

        final String name1 = CACHE1 + mode + writeMode;
        final String name2 = CACHE2 + mode + writeMode;
        final String name3 = CACHE3 + mode + writeMode;

        final IgniteCache<Integer, Integer> cache1 = node.createCache(cacheConfiguration(name1, mode, writeMode));
        final IgniteCache<Integer, Integer> cache2 = node.createCache(cacheConfiguration(name2, mode, writeMode));

        final CountDownLatch sync = new CountDownLatch(1);

        putTestEntries(cache1);
        putTestEntries(cache2);

        IgniteInternalFuture<Void> fut = runAsync(new Callable<Void>() {
            @Override public Void call() throws IgniteInterruptedCheckedException {
                Lock lock = cache1.lock(2);

                lock.lock();

                try {
                    final AffinityTopologyVersion currTop = exchange.readyAffinityVersion();

                    sync.countDown();

                    // Await for new topology.
                    boolean done = GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return currTop.compareTo(exchange.lastTopologyFuture().initialVersion()) < 0;
                        }
                    }, CACHE_CREATION_TIMEOUT);

                    assertTrue(done);

                    // Execute distributed (topology version aware) task.
                    int size = cache2.size();

                    assertEquals(CACHE_SIZE, size);

                    cache2.clear();

                    assertEquals(0, cache2.size());
                }
                finally {
                    lock.unlock();
                }

                return null;
            }
        });

        // Await new thread.
        sync.await(PENDING_TIMEOUT, TimeUnit.MILLISECONDS);

        // Create cache - topology update.
        node.createCache(name3);

        // Await completion or timeout.
        fut.get(PENDING_TIMEOUT);
    }

    /**
     * Test execution within transaction.
     *
     * @param mode {@link CacheMode Cache mode.}
     * @param writeMode {@link CacheWriteSynchronizationMode Cache write synchronization mode.}
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     * @throws CacheException If compute task execution was rejected.
     */
    private void doTestTx(final CacheMode mode, final CacheWriteSynchronizationMode writeMode,
        final TransactionConcurrency concurrency, final TransactionIsolation isolation) throws Exception {
        final Ignite node = grid(0);

        final GridCachePartitionExchangeManager exchange = grid(0).context().cache().context().exchange();

        final String name1 = CACHE1 + mode + writeMode + concurrency + isolation;
        final String name2 = CACHE2 + mode + writeMode + concurrency + isolation;
        final String name3 = CACHE3 + mode + writeMode + concurrency + isolation;

        final IgniteCache<Integer, Integer> cache1 = node.createCache(cacheConfiguration(name1, mode, writeMode));
        final IgniteCache<Integer, Integer> cache2 = node.createCache(cacheConfiguration(name2, mode, writeMode));

        final CountDownLatch sync = new CountDownLatch(1);

        putTestEntries(cache1);
        putTestEntries(cache2);

        IgniteInternalFuture<Void> fut = runAsync(new Callable<Void>() {
            @Override public Void call() throws IgniteInterruptedCheckedException {
                try (Transaction tx = node.transactions().txStart(concurrency, isolation)) {
                    final AffinityTopologyVersion currTop = exchange.readyAffinityVersion();

                    cache1.put(12, 12);

                    sync.countDown();

                    // Await for new topology.
                    boolean done = GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            return currTop.compareTo(exchange.lastTopologyFuture().initialVersion()) < 0;
                        }
                    }, CACHE_CREATION_TIMEOUT);

                    assertTrue(done);

                    // Execute distributed (topology version aware) task.
                    int size = cache2.size();

                    assertEquals(CACHE_SIZE, size);

                    cache2.clear();

                    assertEquals(0, cache2.size());

                    tx.commit();
                }

                return null;
            }
        });

        // Await new thread.
        sync.await(PENDING_TIMEOUT, TimeUnit.MILLISECONDS);

        // Create cache - topology update.
        node.createCache(cacheConfiguration(name3, mode, writeMode));

        // Await completion.
        fut.get(PENDING_TIMEOUT);
    }

    /**
     * Put test keys to cache.
     *
     * @param cache Target cache.
     */
    private void putTestEntries(Cache<Integer, Integer> cache) {
        for (int i = 0; i < CACHE_SIZE; i++)
            cache.put(i, i);
    }
}