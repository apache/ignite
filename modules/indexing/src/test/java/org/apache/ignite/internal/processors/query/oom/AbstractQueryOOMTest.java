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

package org.apache.ignite.internal.processors.query.oom;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for OOME on query.
 */
public abstract class AbstractQueryOOMTest extends GridCommonAbstractTest {
    /** */
    private static final long KEY_CNT = 1_000_000L;

    /** */
    private static final int BATCH_SIZE = 10_000;

    /** */
    private static final String CACHE_NAME = "test_cache";

    /** */
    private static final String HAS_CACHE = "HAS_CACHE";

    /** */
    private static final int RMT_NODES_CNT = 3;

    /** */
    private static final long HANG_TIMEOUT = 15 * 60 * 1000;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30 * 60 * 1000; // 30 mins
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Arrays.asList("-Xmx64m", "-Xms64m");
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        return igniteInstanceName.startsWith("remote");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setCacheConfiguration(new CacheConfiguration()
                .setName(CACHE_NAME)
                .setNodeFilter(new TestNodeFilter())
                .setBackups(0)
                .setQueryParallelism(queryParallelism())
                .setQueryEntities(Collections.singleton(new QueryEntity()
                    .setTableName("test")
                    .setKeyFieldName("ID")
                    .setValueType(Value.class.getName())
                    .addQueryField("ID", Long.class.getName(), null)
                    .addQueryField("INDEXED", Long.class.getName(), null)
                    .addQueryField("VAL", Long.class.getName(), null)
                    .addQueryField("STR", String.class.getName(), null)
                    .setIndexes(Collections.singleton(new QueryIndex("INDEXED"))))))
            .setUserAttributes(igniteInstanceName.startsWith("remote") ? F.asMap(HAS_CACHE, true) : null);
    }

    /**
     * @return query parallelism value.
     */
    protected abstract int queryParallelism();

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        Ignite local = startGrid(0);

        for (int i = 0; i < RMT_NODES_CNT; ++i)
            startGrid("remote-" + i);

        local.cluster().active(true);

        IgniteCache c = local.cache(CACHE_NAME);

        Map<Long, Value> batch = new HashMap<>(BATCH_SIZE);

        for (long i = 0; i < KEY_CNT; ++i) {
            batch.put(i, new Value(i));

            if (batch.size() >= BATCH_SIZE) {
                c.putAll(batch);

                batch.clear();
            }

            if (i % 100_000 == 0)
                log.info("Populate " + i + " values");
        }

        if (!batch.isEmpty()) {
            c.putAll(batch);

            batch.clear();
        }

        awaitPartitionMapExchange(true, true, null);

        local.cluster().active(false);

        stopAllGrids(false);

        IgniteProcessProxy.killAll();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cleanPersistenceDir();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        log.info("Restart cluster");

        Ignite loc = startGrid(0);

        for (int i = 0; i < RMT_NODES_CNT; ++i)
            startGrid("remote-" + i);

        loc.cluster().active(true);

        stopGrid(0, false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(false);

        IgniteProcessProxy.killAll();

        super.afterTest();
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testHeavyScanLazy() throws Exception {
        checkQuery("SELECT * from test", KEY_CNT, true);
    }

    /**
     * @throws Exception On error.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9480")
    @Test
    public void testHeavyScanNonLazy() throws Exception {
        checkQueryExpectOOM("SELECT * from test", false);
    }

    /**
     * OOM on reduce. See IGNITE-9933
     * @throws Exception On error.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9933")
    @Test
    public void testHeavySortByPkLazy() throws Exception {
        checkQueryExpectOOM("SELECT * from test ORDER BY id", true);
    }

    /**
     * @throws Exception On error.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9480")
    @Test
    public void testHeavySortByPkNotLazy() throws Exception {
        checkQueryExpectOOM("SELECT * from test ORDER BY id", false);
    }

    /**
     * OOM on reduce. See IGNITE-9933
     * @throws Exception On error.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9933")
    @Test
    public void testHeavySortByIndexLazy() throws Exception {
        checkQueryExpectOOM("SELECT * from test ORDER BY indexed", true);
    }

    /**
     * @throws Exception On error.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9480")
    @Test
    public void testHeavySortByIndexNotLazy() throws Exception {
        checkQueryExpectOOM("SELECT * from test ORDER BY indexed", false);
    }

    /**
     * @throws Exception On error.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9480")
    @Test
    public void testHeavySortByNotIndexLazy() throws Exception {
        checkQueryExpectOOM("SELECT * from test ORDER BY STR", true);
    }

    /**
     * @throws Exception On error.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9480")
    @Test
    public void testHeavySortByNotIndexNotLazy() throws Exception {
        checkQueryExpectOOM("SELECT * from test ORDER BY str", false);
    }

    /**
     * @throws Exception On error.
     */
    @Test
    public void testHeavyGroupByPkLazy() throws Exception {
        checkQuery("SELECT id, sum(val) from test GROUP BY id", KEY_CNT, true, true);
    }

    /**
     * @throws Exception On error.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9480")
    @Test
    public void testHeavyGroupByPkNotLazy() throws Exception {
        checkQueryExpectOOM("SELECT id, sum(val) from test GROUP BY id", false, true);
    }

    /**
     * @param sql Query.
     * @param lazy Lazy mode.
     * @throws Exception On error.
     */
    private void checkQueryExpectOOM(String sql, boolean lazy) throws Exception {
        checkQueryExpectOOM(sql, lazy, false);
    }

    /**
     * @param sql Query.
     * @param lazy Lazy mode.
     * @param collocated Collocated GROUP BY mode.
     * @throws Exception On error.
     */
    private void checkQueryExpectOOM(String sql, boolean lazy, boolean collocated) throws Exception {
        final AtomicBoolean hangTimeout = new AtomicBoolean();
        final AtomicBoolean hangCheckerEnd = new AtomicBoolean();

        // Start grid hang checker.
        // In some cases grid hangs (e.g. when OOME is thrown at the discovery thread).
        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            try {
                long startTime = U.currentTimeMillis();

                while (!hangCheckerEnd.get() && U.currentTimeMillis() - startTime < HANG_TIMEOUT)
                    U.sleep(1000);

                if (hangCheckerEnd.get())
                    return;

                hangTimeout.set(true);

                log.info("Kill hung grids");

                stopAllGrids();
            }
            catch (IgniteInterruptedCheckedException e) {
                fail("Unexpected interruption");
            }
        });

        try {
            checkQuery(sql, 0, lazy, collocated);

            fail("Query is not produce OOM");
        }
        catch (Exception e) {
            if (hangTimeout.get()) {
                log.info("Grid hangs");

                return;
            }

            if (e.getMessage().contains("Failed to execute SQL query. Out of memory"))
                log.info("OOME is thrown");
            else if (e.getMessage().contains("Failed to communicate with Ignite cluster"))
                log.info("Node is down");
            else
                log.warning("Other error with OOME cause", e);
        }
        finally {
            hangCheckerEnd.set(true);

            fut.get();
        }
    }

    /**
     * @param sql Query.
     * @param expectedRowCnt Expected row count.
     * @param lazy Lazy mode.
     * @throws Exception On failure.
     */
    public void checkQuery(String sql, long expectedRowCnt, boolean lazy) throws Exception {
        checkQuery(sql, expectedRowCnt, lazy, false);
    }

    /**
     * @param sql Query.
     * @param expectedRowCnt Expected row count.
     * @param lazy Lazy mode.
     * @param collocated Collocated group by flag.
     * @throws Exception On failure.
     */
    public void checkQuery(String sql, long expectedRowCnt, boolean lazy, boolean collocated) throws Exception {
        try (Connection c = DriverManager.getConnection(
            "jdbc:ignite:thin://127.0.0.1:10800..10850/\"test_cache\"" +
                "?collocated=" + collocated +
                "&lazy=" + lazy)) {
            try (Statement stmt = c.createStatement()) {
                log.info("Run heavy query: " + sql);

                stmt.execute(sql);

                ResultSet rs = stmt.getResultSet();

                long cnt = 0;
                while (rs.next())
                    cnt++;

                assertEquals("Invalid row count:", expectedRowCnt, cnt);
            }
        }
    }

    /** */
    public static class Value {
        /** Secondary ID. */
        @QuerySqlField(index = true)
        private long indexed;

        /** Secondary ID. */
        @QuerySqlField
        private long val;

        /** String value. */
        @QuerySqlField
        private String str;

        /**
         * @param id ID.
         */
        public Value(long id) {
            indexed = id / 10;
            val = id;
            str = "value " + id;
        }
    }

    /**
     *
     */
    public static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.attribute(HAS_CACHE) != null;
        }
    }
}
