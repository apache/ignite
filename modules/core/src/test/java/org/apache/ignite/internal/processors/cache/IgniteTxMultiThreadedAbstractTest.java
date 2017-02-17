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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests for local transactions.
 */
@SuppressWarnings( {"BusyWait"})
public abstract class IgniteTxMultiThreadedAbstractTest extends IgniteTxAbstractTest {
    /**
     * @return Thread count.
     */
    protected abstract int threadCount();

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If check failed.
     */
    protected void checkCommitMultithreaded(final TransactionConcurrency concurrency,
        final TransactionIsolation isolation) throws Exception {
        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                Thread t = Thread.currentThread();

                t.setName(t.getName() + "-id-" + t.getId());

                info("Starting commit thread: " + Thread.currentThread().getName());

                try {
                    checkCommit(concurrency, isolation);
                }
                finally {
                    info("Finished commit thread: " + Thread.currentThread().getName());
                }

                return null;
            }
        }, threadCount(), concurrency + "-" + isolation);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If check failed.
     */
    protected void checkRollbackMultithreaded(final TransactionConcurrency concurrency,
        final TransactionIsolation isolation) throws Exception {
        final ConcurrentMap<Integer, String> map = new ConcurrentHashMap<>();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                Thread t = Thread.currentThread();

                t.setName(t.getName() + "-id-" + t.getId());

                info("Starting rollback thread: " + Thread.currentThread().getName());

                try {
                    checkRollback(map, concurrency, isolation);

                    return null;
                }
                finally {
                    info("Finished rollback thread: " + Thread.currentThread().getName());
                }
            }
        }, threadCount(), concurrency + "-" + isolation);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticReadCommittedCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(PESSIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticRepeatableReadCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticSerializableCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(PESSIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticReadCommittedCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(OPTIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticRepeatableReadCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(OPTIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticSerializableCommitMultithreaded() throws Exception {
        checkCommitMultithreaded(OPTIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticReadCommittedRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(PESSIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticRepeatableReadRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticSerializableRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(PESSIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticReadCommittedRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(OPTIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticRepeatableReadRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(OPTIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticSerializableRollbackMultithreaded() throws Exception {
        checkRollbackMultithreaded(OPTIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableConsistency() throws Exception {
        final IgniteCache<Integer, Long> cache = grid(0).cache(null);

        final int THREADS = 3;

        final int ITERATIONS = 100;

        for (int key0 = 100_000; key0 < 100_000 + 20; key0++) {
            final int key = key0;

            cache.put(key, 0L);

            List<IgniteInternalFuture<Collection<Long>>> futs = new ArrayList<>(THREADS);

            for (int i = 0; i < THREADS; i++) {
                futs.add(GridTestUtils.runAsync(new Callable<Collection<Long>>() {
                    @Override public Collection<Long> call() throws Exception {
                        Collection<Long> res = new ArrayList<>();

                        for (int i = 0; i < ITERATIONS; i++) {
                            while (true) {
                                try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                                    long val = cache.get(key);

                                    cache.put(key, val + 1);

                                    tx.commit();

                                    assertTrue(res.add(val + 1));

                                    break;
                                }
                                catch (TransactionOptimisticException ignored) {
                                    // Retry.
                                }
                            }
                        }

                        return res;
                    }
                }));
            }

            long total = 0;

            List<Collection<Long>> cols = new ArrayList<>(THREADS);

            for (IgniteInternalFuture<Collection<Long>> fut : futs) {
                Collection<Long> col = fut.get();

                assertEquals(ITERATIONS, col.size());

                total += col.size();

                cols.add(col);
            }

            log.info("Cache value: " + cache.get(key));

            Set<Long> duplicates = new HashSet<>();

            for (Collection<Long> col1 : cols) {
                for (Long val1 : col1) {
                    for (Collection<Long> col2 : cols) {
                        if (col1 == col2)
                            continue;

                        for (Long val2 : col2) {
                            if (val1.equals(val2)) {
                                duplicates.add(val2);

                                break;
                            }
                        }
                    }
                }
            }

            assertTrue("Found duplicated values: " + duplicates, duplicates.isEmpty());

            assertEquals((long)THREADS * ITERATIONS, total);

            // Try to update one more time to make sure cache is in consistent state.
            try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                long val = cache.get(key);

                cache.put(key, val);

                tx.commit();
            }

            for (int i = 0; i < gridCount(); i++)
                assertEquals(total, grid(i).cache(null).get(key));
        }
    }
}
