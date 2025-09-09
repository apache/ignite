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

package org.apache.ignite.internal.processors.query.h2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.query.GridQueryFinishedInfo;
import org.apache.ignite.internal.processors.query.GridQueryStartedInfo;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.QueryUtils.SCHEMA_SYS;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

/** Test for SQL query listeners. */
public class IgniteSqlQueryStartFinishListenerTest extends AbstractIndexingCommonTest {
    /** Client node name. */
    private static final String CLIENT_NODE_NAME = "CLIENT_NODE";

    /** Client node name. */
    private static final String SERVER_NODE_NAME = "SERVER_NODE";

    /** Listeners. */
    private final List<Object> lsnrs = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SERVER_NODE_NAME);
        startClientGrid(CLIENT_NODE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** */
    @After
    public void unregisterListeners() {
        lsnrs.forEach(indexing()::unregisterQueryFinishedListener);
        lsnrs.forEach(indexing()::unregisterQueryStartedListener);

        lsnrs.clear();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setSqlSchemas("TEST1")
            .setCacheConfiguration(
                new CacheConfiguration<String, String>(DEFAULT_CACHE_NAME)
                    .setQueryEntities(Collections.singleton(new QueryEntity(String.class, String.class)))
                    .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
            );
    }

    /**
     * Ensure you could register and unregister a listener for query start/finish events:
     *     - register listeners
     *     - execute a query
     *     - ensure both listeneres were notified
     *     - unregister the query start listener
     *     - run a query one more time
     *     - ensure only one listener was notified
     *     - unregister the query finish listener and register new one
     *     - run a query one more time
     *     - ensure only new listener was notified
     *
     * @throws Exception In case of error.
     */
    @Test
    public void testRegisterUnregisterQueryListeners() throws Exception {
        final AtomicInteger qryStarted = new AtomicInteger();
        final AtomicInteger qryFinished = new AtomicInteger();

        final Consumer<GridQueryStartedInfo> qryStartedLsnr = registerQueryStartedListener(info -> qryStarted.incrementAndGet());
        final Consumer<GridQueryFinishedInfo> qryFinishedLsnr = registerQueryFinishedListener(info -> qryFinished.incrementAndGet());

        {
            execSql(SCHEMA_SYS, "select * from caches");

            assertWithTimeout(qryStarted::get, is(equalTo(1)), 1_000);
            assertWithTimeout(qryFinished::get, is(equalTo(1)), 1_000);
        }

        {
            assertTrue(indexing().unregisterQueryStartedListener(qryStartedLsnr));

            execSql(SCHEMA_SYS, "select * from caches");

            assertWithTimeout(qryFinished::get, is(equalTo(2)), 1_000);
            assertWithTimeout(qryStarted::get, is(equalTo(1)), 1_000);
        }

        {
            assertTrue(indexing().unregisterQueryFinishedListener(qryFinishedLsnr));

            final CountDownLatch latch = new CountDownLatch(1);

            registerQueryFinishedListener(info -> latch.countDown());

            execSql(SCHEMA_SYS, "select * from caches");

            latch.await(1, TimeUnit.SECONDS);

            assertWithTimeout(qryFinished::get, is(equalTo(2)), 1_000);
            assertWithTimeout(qryStarted::get, is(equalTo(1)), 1_000);
        }
    }

    /**
     * Ensure listeners are notified with an actual query info:
     *     - register listeners
     *     - execute different queries
     *     - verify query info passed to listeners
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testVerifyQueryInfoPassedToListeners() throws Exception {
        final AtomicReference<GridQueryStartedInfo> qryStarted = new AtomicReference<>();
        final AtomicReference<GridQueryFinishedInfo> qryFinished = new AtomicReference<>();

        registerQueryStartedListener(qryStarted::set);
        registerQueryFinishedListener(qryFinished::set);

        {
            final long delay = 100;
            final String qry = "select * from caches where ? = \"default\".sleep(?) limit 1";

            execSql(SCHEMA_SYS, qry, delay, delay);

            assertWithTimeout(qryStarted::get, is(notNullValue()), 1_000);

            GridQueryStartedInfo startedInfo = qryStarted.get();
            assertEquals(SCHEMA_SYS, startedInfo.schemaName());
            assertEquals(qry, startedInfo.query());
            assertEquals(grid(SERVER_NODE_NAME).localNode().id(), startedInfo.nodeId());
            assertEquals(false, startedInfo.local());
            assertEquals(GridCacheQueryType.SQL_FIELDS, startedInfo.queryType());

            assertWithTimeout(qryFinished::get, is(notNullValue()), 1_000);

            GridQueryFinishedInfo finishedInfo = qryFinished.get();
            assertEquals(SCHEMA_SYS, finishedInfo.schemaName());
            assertEquals(qry, finishedInfo.query());
            assertEquals(grid(SERVER_NODE_NAME).localNode().id(), startedInfo.nodeId());
            assertEquals(false, finishedInfo.local());
            assertEquals(GridCacheQueryType.SQL_FIELDS, finishedInfo.queryType());
            assertEquals(false, finishedInfo.failed());
            assertThat(finishedInfo.finishTime() - finishedInfo.startTime(), is(greaterOrEqualTo(delay)));

            qryStarted.set(null);
            qryFinished.set(null);
        }

        {
            final String schema = "TEST1";
            final String qry = "select \"default\".can_fail(?) from " + SCHEMA_SYS + ".caches where ? = ? limit 1";

            GridTestUtils.assertThrowsWithCause(() -> execSqlLocal(schema, qry, true, 1, 1), IgniteSQLException.class);

            assertWithTimeout(qryStarted::get, is(notNullValue()), 1_000);

            GridQueryStartedInfo startedInfo = qryStarted.get();
            assertEquals(schema, startedInfo.schemaName());
            assertEquals(qry, startedInfo.query());
            assertEquals(grid(SERVER_NODE_NAME).localNode().id(), startedInfo.nodeId());
            assertEquals(true, startedInfo.local());
            assertEquals(GridCacheQueryType.SQL_FIELDS, startedInfo.queryType());

            assertWithTimeout(qryFinished::get, is(notNullValue()), 1_000);

            GridQueryFinishedInfo finishedInfo = qryFinished.get();
            assertEquals(schema, finishedInfo.schemaName());
            assertEquals(qry, finishedInfo.query());
            assertEquals(grid(SERVER_NODE_NAME).localNode().id(), startedInfo.nodeId());
            assertEquals(true, finishedInfo.local());
            assertEquals(GridCacheQueryType.SQL_FIELDS, finishedInfo.queryType());
            assertEquals(true, finishedInfo.failed());
            assertThat(finishedInfo.finishTime(), is(greaterOrEqualTo(finishedInfo.startTime())));

            qryStarted.set(null);
            qryFinished.set(null);
        }

        {
            final String qry = "text query";

            IgniteCache<String, Object> cache = grid(CLIENT_NODE_NAME).cache(DEFAULT_CACHE_NAME);

            cache.query(new TextQuery<String, String>(String.class, "text query"));

            assertWithTimeout(qryStarted::get, is(notNullValue()), 1_000);

            GridQueryStartedInfo startedInfo = qryStarted.get();
            assertEquals(cache.getName(), startedInfo.schemaName());
            assertEquals(qry, startedInfo.query());
            assertEquals(grid(SERVER_NODE_NAME).localNode().id(), startedInfo.nodeId());
            assertEquals(true, startedInfo.local());
            assertEquals(GridCacheQueryType.TEXT, startedInfo.queryType());

            assertWithTimeout(qryFinished::get, is(notNullValue()), 1_000);

            GridQueryFinishedInfo finishedInfo = qryFinished.get();
            assertEquals(cache.getName(), finishedInfo.schemaName());
            assertEquals(qry, finishedInfo.query());
            assertEquals(grid(SERVER_NODE_NAME).localNode().id(), startedInfo.nodeId());
            assertEquals(true, finishedInfo.local());
            assertEquals(GridCacheQueryType.TEXT, finishedInfo.queryType());
            assertEquals(false, finishedInfo.failed());
            assertThat(finishedInfo.finishTime(), is(greaterOrEqualTo(finishedInfo.startTime())));

            qryStarted.set(null);
            qryFinished.set(null);
        }
    }

    /**
     * Ensure listeners do not block query execution
     *     - register blocking listeners
     *     - execute a lot of queries
     *     - verify all queries finished while listeners is still blocked
     */
    @Test
    public void testListeneresNotBlocksQueryExecution() throws IgniteCheckedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger lsnrCalls = new AtomicInteger();

        final int quryRuns = 1_000;
        final int threadCnt = 20;

        registerQueryStartedListener(info -> {
            try {
                latch.await();
            }
            catch (InterruptedException ignored) {
            }

            lsnrCalls.incrementAndGet();
        });
        registerQueryFinishedListener(info -> {
            try {
                latch.await();
            }
            catch (InterruptedException ignored) {
            }

            lsnrCalls.incrementAndGet();
        });

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            for (int i = 0; i < quryRuns; i++)
                execSql(SCHEMA_SYS, "select * from caches");
        }, threadCnt, "test-async-query-runner");

        try {
            fut.get(15_000);
        }
        finally {
            latch.countDown();
        }

        assertWithTimeout(lsnrCalls::get, is(equalTo(2 * threadCnt * quryRuns)), 15_000);
    }

    /**
     * Ensure notification chain is not interrupted if exception was thrown in the middle of the chain
     *     - register several listeners such the listener will throw exception if condition is met
     *     - execute query several times, one of the listeners from each chain should fail
     *     - verify all other listeners were notified
     */
    @Test
    public void testFailedListenereNotAffectOthers() throws IgniteCheckedException {
        final int lsnrCnt = 3;
        final long waitTimeout = 1_000;

        boolean[] startLsnrsNotified = new boolean[lsnrCnt];
        boolean[] finishLsnrsNotified = new boolean[lsnrCnt];
        boolean[] startLsnrShouldFail = new boolean[lsnrCnt];
        boolean[] finishLsnrShouldFail = new boolean[lsnrCnt];

        for (int i = 0; i < lsnrCnt; i++) {
            final int lsnNo = i;

            registerQueryStartedListener(info -> {
                if (startLsnrShouldFail[lsnNo]) {
                    startLsnrShouldFail[lsnNo] = false;

                    throw new RuntimeException("Start listener fails");
                }

                startLsnrsNotified[lsnNo] = true;
            });

            registerQueryFinishedListener(info -> {
                if (finishLsnrShouldFail[lsnNo]) {
                    finishLsnrShouldFail[lsnNo] = false;

                    throw new RuntimeException("Finish listener fails");
                }

                finishLsnrsNotified[lsnNo] = true;
            });
        }

        {
            startLsnrShouldFail[0] = true;
            finishLsnrShouldFail[1] = true;

            execSql(SCHEMA_SYS, "select * from caches");

            assertWithTimeout(() -> startLsnrsNotified[0], isFalse(), waitTimeout);
            assertWithTimeout(() -> startLsnrsNotified[1], isTrue(), waitTimeout);
            assertWithTimeout(() -> startLsnrsNotified[2], isTrue(), waitTimeout);

            assertWithTimeout(() -> finishLsnrsNotified[0], isTrue(), waitTimeout);
            assertWithTimeout(() -> finishLsnrsNotified[1], isFalse(), waitTimeout);
            assertWithTimeout(() -> finishLsnrsNotified[2], isTrue(), waitTimeout);

            resetListeners(startLsnrsNotified, finishLsnrsNotified);
        }

        {
            startLsnrShouldFail[1] = true;
            finishLsnrShouldFail[2] = true;

            execSql(SCHEMA_SYS, "select * from caches");

            assertWithTimeout(() -> startLsnrsNotified[0], isTrue(), waitTimeout);
            assertWithTimeout(() -> startLsnrsNotified[1], isFalse(), waitTimeout);
            assertWithTimeout(() -> startLsnrsNotified[2], isTrue(), waitTimeout);

            assertWithTimeout(() -> finishLsnrsNotified[0], isTrue(), waitTimeout);
            assertWithTimeout(() -> finishLsnrsNotified[1], isTrue(), waitTimeout);
            assertWithTimeout(() -> finishLsnrsNotified[2], isFalse(), waitTimeout);

            resetListeners(startLsnrsNotified, finishLsnrsNotified);
        }

        {
            startLsnrShouldFail[2] = true;
            finishLsnrShouldFail[0] = true;

            execSql(SCHEMA_SYS, "select * from caches");

            assertWithTimeout(() -> startLsnrsNotified[0], isTrue(), waitTimeout);
            assertWithTimeout(() -> startLsnrsNotified[1], isTrue(), waitTimeout);
            assertWithTimeout(() -> startLsnrsNotified[2], isFalse(), waitTimeout);

            assertWithTimeout(() -> finishLsnrsNotified[0], isFalse(), waitTimeout);
            assertWithTimeout(() -> finishLsnrsNotified[1], isTrue(), waitTimeout);
            assertWithTimeout(() -> finishLsnrsNotified[2], isTrue(), waitTimeout);

            resetListeners(startLsnrsNotified, finishLsnrsNotified);
        }
    }

    /**
     * Sets all elements from each array to {@code false}.
     */
    private void resetListeners(boolean[]... arrays) {
        for (boolean[] arr : arrays)
            Arrays.fill(arr, false);
    }

    /** */
    private IgniteH2Indexing indexing() {
        return (IgniteH2Indexing)grid(SERVER_NODE_NAME).context().query().getIndexing();
    }

    /** */
    private List<List<?>> execSql(String schema, String sql, Object... args) {
        return grid(SERVER_NODE_NAME).cache(DEFAULT_CACHE_NAME).query(
            new SqlFieldsQuery(sql).setSchema(schema).setArgs(args).setLocal(false)
        ).getAll();
    }

    /** */
    private List<List<?>> execSqlLocal(String schema, String sql, Object... args) {
        return grid(SERVER_NODE_NAME).cache(DEFAULT_CACHE_NAME).query(
            new SqlFieldsQuery(sql).setSchema(schema).setArgs(args).setLocal(true)
        ).getAll();
    }

    /**
     * @param lsnr Listener.
     */
    private Consumer<GridQueryStartedInfo> registerQueryStartedListener(Consumer<GridQueryStartedInfo> lsnr) {
        lsnrs.add(lsnr);

        indexing().registerQueryStartedListener(lsnr);

        return lsnr;
    }

    /**
     * @param lsnr Listener.
     */
    private Consumer<GridQueryFinishedInfo> registerQueryFinishedListener(Consumer<GridQueryFinishedInfo> lsnr) {
        lsnrs.add(lsnr);

        indexing().registerQueryFinishedListener(lsnr);

        return lsnr;
    }

    /**
     * @param actualSupplier Supplier for value to check.
     * @param matcher Matcher.
     * @param timeout Timeout.
     */
    private <T> void assertWithTimeout(Supplier<T> actualSupplier, Matcher<? super T> matcher, long timeout)
        throws IgniteInterruptedCheckedException {
        GridTestUtils.waitForCondition(() -> matcher.matches(actualSupplier.get()), timeout);

        assertThat(actualSupplier.get(), matcher);
    }

    /**
     * @param wanted Wanted.
     */
    private static <T extends Comparable<? super T>> Matcher<T> greaterOrEqualTo(T wanted) {
        return new CustomMatcher<T>("should be greater or equal to " + wanted) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            @Override public boolean matches(Object item) {
                return wanted != null && item instanceof Comparable && ((Comparable)item).compareTo(wanted) >= 0;
            }
        };
    }

    /** */
    private static Matcher<Boolean> isTrue() {
        return new CustomMatcher<Boolean>("should be true ") {
            @Override public boolean matches(Object item) {
                return item instanceof Boolean && (Boolean)item;
            }
        };
    }

    /** */
    private static Matcher<Boolean> isFalse() {
        return new CustomMatcher<Boolean>("should be true ") {
            @Override public boolean matches(Object item) {
                return item instanceof Boolean && !(Boolean)item;
            }
        };
    }
}
