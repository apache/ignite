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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

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
    // TODO: GG-8063, enabled when fixed.
    public void _testOptimisticSerializableConsistency() throws Exception {
        final IgniteCache<Integer, Long> cache = grid(0).cache(null);

        final int THREADS = 2;

        final int ITERATIONS = 100;

        final int key = 0;

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
                            catch(TransactionOptimisticException e) {
                                log.info("Got error, will retry: " + e);
                            }
                        }
                    }

                    return res;
                }
            }));
        }

        List<Collection<Long>> cols = new ArrayList<>(THREADS);

        for (IgniteInternalFuture<Collection<Long>> fut : futs) {
            Collection<Long> col = fut.get();

            assertEquals(ITERATIONS, col.size());

            cols.add(col);
        }

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
    }
}
