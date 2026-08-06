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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.IntFunction;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** Tests that concurrent bulk operations do not deadlock. */
public class BulkOperationDeadlockIntegrationTest extends AbstractBasicIntegrationTest {
    /** Cache used by KeyValue API tests. */
    private static final String KEY_VALUE_CACHE_NAME = "bulk-operation-cache";

    /** Number of entries processed by each bulk operation. */
    private static final int ENTRY_COUNT = 30;

    /** Number of person IDs in each tenant. */
    private static final int PERSONS_PER_TENANT = 3;

    /** Number of concurrent transaction workers. */
    private static final int CONCURRENT_TX_THREADS = 6;

    /** Duration of the concurrent load. */
    private static final long CONCURRENT_TX_DURATION_MS = TimeUnit.MINUTES.toMillis(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setTransactionConfiguration(new TransactionConfiguration().setTxAwareQueriesEnabled(true));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        sql("CREATE TABLE Person (tenantId INT, personId INT, val INT, "
            + "PRIMARY KEY (tenantId, personId)) WITH atomicity=TRANSACTIONAL");

        for (int entry = 0; entry < ENTRY_COUNT; entry++) {
            TestKey key = testKey(entry);

            sql("INSERT INTO Person (tenantId, personId, val) VALUES (?, ?, 0)", key.tenantId, key.personId);
        }

        IgniteCache<TestKey, Integer> cache = client.getOrCreateCache(
            new CacheConfiguration<TestKey, Integer>(KEY_VALUE_CACHE_NAME)
                .setAtomicityMode(TRANSACTIONAL)
                .setBackups(1)
        );

        for (int entry = 0; entry < ENTRY_COUNT; entry++)
            cache.put(testKey(entry), 0);
    }

    /** Concurrent opposite sort orders acquire the same row set without deadlocks. */
    @Test
    public void testSelectForUpdateWithOppositeOrderDoesNotDeadlock() {
        runConcurrentBulkOperation(
            READ_COMMITTED,
            workerId -> "SELECT FOR UPDATE [order=" + keyOrder(workerId) + ']',
            (node, workerId) -> {
                boolean ascending = ascending(workerId);
                String order = keyOrder(workerId);
                List<List<?>> rows = sql(node, "SELECT tenantId, personId FROM Person "
                    + "ORDER BY tenantId " + order + ", personId " + order + " FOR UPDATE WAIT 5");

                assertEquals(ENTRY_COUNT, rows.size());

                for (int row = 0; row < rows.size(); row++) {
                    int entry = ascending ? row : rows.size() - row - 1;
                    TestKey expectedKey = testKey(entry);

                    assertEquals(Integer.valueOf(expectedKey.tenantId), rows.get(row).get(0));
                    assertEquals(Integer.valueOf(expectedKey.personId), rows.get(row).get(1));
                }
            }
        );
    }

    /** Concurrent updates lock rows produced by an unordered scan without deadlocks. */
    @Ignore("The deadlock issue for this bulk operation has not been resolved yet.")
    @Test
    public void testUpdateWithWhereDoesNotDeadlock() {
        runConcurrentBulkOperation(
            READ_COMMITTED,
            workerId -> "UPDATE with WHERE",
            (node, workerId) -> {
                List<List<?>> rows = sql(node, "UPDATE Person SET val = val + 1 WHERE val >= 0");

                assertEquals(1, rows.size());
                assertEquals(1, rows.get(0).size());
                assertEquals(Long.valueOf(ENTRY_COUNT), rows.get(0).get(0));
            }
        );
    }

    /** Concurrent putAll operations lock keys in opposite map iteration orders without deadlocks. */
    @Ignore("The deadlock issue for this bulk operation has not been resolved yet.")
    @Test
    public void testPutAllWithOppositeKeyOrderDoesNotDeadlock() {
        runConcurrentBulkOperation(
            REPEATABLE_READ,
            workerId -> "putAll [order=" + keyOrder(workerId) + ']',
            (node, workerId) -> {
                IgniteCache<TestKey, Integer> cache = node.cache(KEY_VALUE_CACHE_NAME);
                Map<TestKey, Integer> entries = new TreeMap<>(keyComparator(workerId));

                for (int entry = 0; entry < ENTRY_COUNT; entry++)
                    entries.put(testKey(entry), workerId);

                cache.putAll(entries);
            }
        );
    }

    /** Concurrent getAll operations lock keys in opposite set iteration orders without deadlocks. */
    @Ignore("The deadlock issue for this bulk operation has not been resolved yet.")
    @Test
    public void testGetAllWithOppositeKeyOrderDoesNotDeadlock() {
        runConcurrentBulkOperation(
            REPEATABLE_READ,
            workerId -> "getAll [order=" + keyOrder(workerId) + ']',
            (node, workerId) -> {
                IgniteCache<TestKey, Integer> cache = node.cache(KEY_VALUE_CACHE_NAME);
                Set<TestKey> keys = new TreeSet<>(keyComparator(workerId));

                for (int entry = 0; entry < ENTRY_COUNT; entry++)
                    keys.add(testKey(entry));

                assertEquals(ENTRY_COUNT, cache.getAll(keys).size());
            }
        );
    }

    /** Runs the given bulk operation concurrently in pessimistic transactions. */
    private void runConcurrentBulkOperation(
        TransactionIsolation isolation,
        IntFunction<String> operationName,
        BulkOperation operation
    ) {
        CountDownLatch ready = new CountDownLatch(CONCURRENT_TX_THREADS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger workerIdGenerator = new AtomicInteger();
        AtomicLong endNanos = new AtomicLong();
        AtomicLongArray completed = new AtomicLongArray(CONCURRENT_TX_THREADS);
        Queue<Throwable> errors = new ConcurrentLinkedQueue<>();
        IgniteEx[] nodes = {client, grid(0), grid(1)};

        IgniteInternalFuture<?> workers = GridTestUtils.runMultiThreadedAsync(() -> {
            int workerId = workerIdGenerator.getAndIncrement();
            IgniteEx node = nodes[workerId % nodes.length];
            String name = operationName.apply(workerId);

            ready.countDown();

            try {
                if (!start.await(10, TimeUnit.SECONDS))
                    throw new AssertionError("Timed out waiting for concurrent bulk operation start");

                while (!stop.get() && System.nanoTime() < endNanos.get()) {
                    try (Transaction tx = node.transactions().txStart(
                        PESSIMISTIC,
                        isolation,
                        TimeUnit.SECONDS.toMillis(10),
                        0
                    )) {
                        operation.execute(node, workerId);

                        // Keep the locks briefly so the other workers continuously contend for the same entries.
                        Thread.sleep(10);

                        tx.commit();
                    }

                    completed.incrementAndGet(workerId);
                }
            }
            catch (Throwable t) {
                errors.add(new AssertionError(
                    "Concurrent bulk operation worker failed [worker=" + workerId + ", operation=" + name
                        + ", node=" + node.name() + ']',
                    t
                ));

                stop.set(true);
            }
        }, CONCURRENT_TX_THREADS, "bulk-operation-deadlock");

        try {
            if (!ready.await(10, TimeUnit.SECONDS))
                errors.add(new AssertionError("Not all concurrent bulk operation workers became ready"));
            else {
                endNanos.set(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CONCURRENT_TX_DURATION_MS));
                start.countDown();

                workers.get(CONCURRENT_TX_DURATION_MS + TimeUnit.SECONDS.toMillis(20));
            }
        }
        catch (Throwable t) {
            errors.add(new AssertionError("Concurrent bulk operation workers did not finish in time", t));
        }
        finally {
            stop.set(true);
            start.countDown();

            try {
                workers.get(TimeUnit.SECONDS.toMillis(10));
            }
            catch (Throwable t) {
                errors.add(new AssertionError("Failed to stop concurrent bulk operation workers", t));
            }
        }

        if (!errors.isEmpty()) {
            AssertionError failure = new AssertionError(
                "Concurrent bulk operation failed [errors=" + errors.size() + ", completed=" + completed + ']');

            errors.forEach(failure::addSuppressed);

            throw failure;
        }

        for (int workerId = 0; workerId < CONCURRENT_TX_THREADS; workerId++) {
            assertTrue(
                "No transaction completed [worker=" + workerId + ", operation=" + operationName.apply(workerId) + ']',
                completed.get(workerId) > 0
            );
        }
    }

    /** Returns whether the worker uses ascending key order. */
    private static boolean ascending(int workerId) {
        return workerId % 2 == 0;
    }

    /** Returns the key order used by the worker. */
    private static String keyOrder(int workerId) {
        return ascending(workerId) ? "ASC" : "DESC";
    }

    /** Returns a comparator defining the worker's key order. */
    private static Comparator<TestKey> keyComparator(int workerId) {
        return ascending(workerId) ? Comparator.naturalOrder() : Comparator.reverseOrder();
    }

    /** Returns the composite key for a zero-based entry index. */
    private static TestKey testKey(int entry) {
        return new TestKey(entry / PERSONS_PER_TENANT + 1, entry % PERSONS_PER_TENANT + 1);
    }

    /** Bulk operation executed by a worker transaction. */
    @FunctionalInterface
    private interface BulkOperation {
        /** Executes the operation. */
        void execute(IgniteEx node, int workerId) throws Exception;
    }

    /** Composite cache key used by bulk operations. */
    private static class TestKey implements Serializable, Comparable<TestKey> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Tenant ID. */
        private final int tenantId;

        /** Person ID within the tenant. */
        private final int personId;

        /**
         * @param tenantId Tenant ID.
         * @param personId Person ID within the tenant.
         */
        private TestKey(int tenantId, int personId) {
            this.tenantId = tenantId;
            this.personId = personId;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(TestKey other) {
            int cmp = Integer.compare(tenantId, other.tenantId);

            return cmp != 0 ? cmp : Integer.compare(personId, other.personId);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (this == obj)
                return true;

            if (obj == null || getClass() != obj.getClass())
                return false;

            TestKey other = (TestKey)obj;

            return tenantId == other.tenantId && personId == other.personId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 31 * tenantId + personId;
        }
    }
}
