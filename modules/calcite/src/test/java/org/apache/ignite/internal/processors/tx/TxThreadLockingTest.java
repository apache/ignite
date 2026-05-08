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

package org.apache.ignite.internal.processors.tx;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AbstractExecutionTest;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** */
@RunWith(Parameterized.class)
public class TxThreadLockingTest extends AbstractBasicIntegrationTest {
    /** */
    private static final long TIMEOUT = 10_000L;

    /** */
    private static final int POOL_SIZE = 10;

    /** */
    private static final int IN_BUFFER_SIZE = AbstractExecutionTest.IN_BUFFER_SIZE;

    /** */
    private static final int MODIFY_BATCH_SIZE = AbstractExecutionTest.MODIFY_BATCH_SIZE;

    /** Use query blocking executor. */
    @Parameterized.Parameter(0)
    public boolean qryBlockingExecutor;

    /** */
    @Parameterized.Parameters(name = "qryBlockingExecutor={0}")
    public static Collection<?> parameters() {
        return List.of(false, true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR, String.valueOf(qryBlockingExecutor));

        startGrid(0);

        client = startClientGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        System.clearProperty(IGNITE_CALCITE_USE_QUERY_BLOCKING_TASK_EXECUTOR);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setQueryThreadPoolSize(POOL_SIZE);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> cacheConfiguration() {
        return super.<K, V>cacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    @Test
    public void testThreadsLockingByDml() throws Exception {
        createAndPopulateTable();

        CountDownLatch unlockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> lockKeyFut = lockKeyAndWaitForLatch(0, unlockLatch);

        IgniteInternalFuture<?> updFut = GridTestUtils.runMultiThreadedAsync(() -> {
            sql("UPDATE person SET salary = 1 WHERE id = 0");
        }, POOL_SIZE + 1, "update-thread");

        // Ensure update queries are blocked.
        assertFalse(GridTestUtils.waitForCondition(updFut::isDone, 200));

        IgniteInternalFuture<?> selectFut = GridTestUtils.runMultiThreadedAsync(() -> {
            for (int i = 0; i < 100; i++)
                sql("SELECT * FROM person WHERE id = 1");
        }, POOL_SIZE + 1, "select-thread");

        // Ensure other queries are not blocked by update queries.
        selectFut.get(TIMEOUT, TimeUnit.MILLISECONDS);

        unlockLatch.countDown();

        lockKeyFut.get();

        // Ensure update queries are released.
        updFut.get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /** */
    @Test
    public void testDifferentBlockedBatchSize() throws Exception {
        assertTrue("Unexpected constants [MODIFY_BATCH_SIZE=" + MODIFY_BATCH_SIZE +
            ", IN_BUFFER_SIZE=" + IN_BUFFER_SIZE + ']', MODIFY_BATCH_SIZE < IN_BUFFER_SIZE);

        createAndPopulateTable();

        IgniteCache<Integer, Employer> cache = client.cache(TABLE_NAME);

        for (int i = 0; i < IN_BUFFER_SIZE + 1; i++)
            cache.put(i, new Employer("Test" + i, (double)i));

        for (int size : new int[] {MODIFY_BATCH_SIZE, MODIFY_BATCH_SIZE + 1, IN_BUFFER_SIZE, IN_BUFFER_SIZE + 1}) {
            log.info("Blocked batch size: " + size);

            CountDownLatch unlockLatch = new CountDownLatch(1);

            IgniteInternalFuture<?> lockKeyFut = lockKeyAndWaitForLatch(0, unlockLatch);

            IgniteInternalFuture<List<List<?>>> updFut = GridTestUtils.runAsync(() ->
                sql("UPDATE person SET salary = 1 WHERE id < ?", size));

            // Ensure update query is blocked.
            assertFalse(GridTestUtils.waitForCondition(updFut::isDone, 200));

            unlockLatch.countDown();

            lockKeyFut.get();

            List<List<?>> res = updFut.get(TIMEOUT, TimeUnit.MILLISECONDS);

            assertEquals((long)size, res.get(0).get(0));
        }
    }

    /** */
    @Test
    public void testErrorAfterAsyncWait() throws Exception {
        createAndPopulateTable();

        CountDownLatch unlockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> lockKeyFut = lockKeyAndWaitForLatch(0, unlockLatch);

        IgniteInternalFuture<List<List<?>>> insFut = GridTestUtils.runAsync(() ->
            sql("INSERT INTO person (id, name, salary) VALUES (0, 'Test', 0)"));

        // Ensure insert query is blocked.
        assertFalse(GridTestUtils.waitForCondition(insFut::isDone, 200));

        unlockLatch.countDown();

        lockKeyFut.get();

        // Ensure exception is propagated to query initiator after async batch execution finished.
        try {
            insFut.get(TIMEOUT, TimeUnit.MILLISECONDS);

            fail("Exception wasn't thrown");
        }
        catch (IgniteCheckedException expected) {
            assertTrue(X.hasCause(expected, "Failed to INSERT some keys because they are already in cache",
                IgniteSQLException.class));
        }
    }

    /** */
    private IgniteInternalFuture<?> lockKeyAndWaitForLatch(int key, CountDownLatch unlockLatch) throws Exception {
        CountDownLatch lockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> lockKeyFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                client.cache(TABLE_NAME).put(key, new Employer("Test", 0d));

                lockLatch.countDown();

                assertTrue(unlockLatch.await(TIMEOUT, TimeUnit.MILLISECONDS));

                tx.commit();
            }
        });

        assertTrue(lockLatch.await(TIMEOUT, TimeUnit.MILLISECONDS));

        return lockKeyFut;
    }
}
