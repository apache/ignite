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

package org.apache.ignite.internal.processors.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 * Tests for OOME on query.
 */
public class LazyQueryOOMTest extends GridCommonAbstractTest {
    /** */
    private static final int KEY_CNT = 1000;

    /** */
    private static final String CACHE_NAME = "test_cache";

    /** */
    private static final String HAS_CACHE = "HAS_CACHE";

    /** */
    private static final int RMT_NODES_CNT = 3;

    /** */
    private static final long HANG_TIMEOUT =  5 * 60 * 1000;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30 * 60 * 1000; // 30 mins
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Arrays.asList("-Xmx128m");
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        return igniteInstanceName.contains("remote");
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
                .setBackups(1)
                .setQueryEntities(Collections.singleton(new QueryEntity()
                    .setTableName("test")
                    .setKeyFieldName("ID")
                    .setValueType(Value.class.getName())
                    .addQueryField("ID", Long.class.getName(), null)
                    .addQueryField("SECID", Long.class.getName(), null)
                    .addQueryField("STR", String.class.getName(), null))))
            .setUserAttributes(igniteInstanceName.startsWith("remote") ? F.asMap(HAS_CACHE, true) : null)
            .setClientMode(igniteInstanceName.startsWith("client"));

    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cleanPersistenceDir();

        stopAllGrids();

        IgniteProcessProxy.killAll();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignite local = startGrid(0);

        for (int i = 0; i < RMT_NODES_CNT; ++i)
            startGrid("remote-" + i);

        local.cluster().active(true);

        try (IgniteDataStreamer streamer = local.dataStreamer(CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (long i = 0; i < KEY_CNT; ++i)
                streamer.addData(i, new Value(i));
        }

        awaitPartitionMapExchange();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        Ignite local = startGrid(0);

        for (int i = 0; i < RMT_NODES_CNT; ++i)
            startGrid("remote-" + i);

        local.cluster().active(true);

        stopGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IgniteProcessProxy.killAll();

        stopAllGrids();
    }

    /**
     * @throws Exception On error.
     */
    public void testHeavyJoinLazy() throws Exception {
        checkHeavyJoin(true);
    }

    /**
     * @throws Exception On error.
     */
    public void testHeavyJoinNotLazy() throws Exception {
        final AtomicBoolean hangTimeout = new AtomicBoolean();
        final AtomicBoolean hangCheckerEnd = new AtomicBoolean();

        try {

            // Start grid hang checker.
            // In some cases grid hangs (e.g. when OOME is thrown at the discovery thread).
            GridTestUtils.runAsync(() -> {
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
                checkHeavyJoin(false);
            }
            catch (Exception e) {
                e.printStackTrace();

                if (!hangTimeout.get()) {
                    assertTrue(e instanceof SQLException);
                    assertTrue(e.getMessage().contains("Failed to execute SQL query. Out of memory"));
                }
                else
                    log.info("Grid hangs");
            }
        }
        finally {
            hangCheckerEnd.set(true);
        }
    }

    /**
     * Executes heavy join. Result set is invalid because data must be collocated.
     * But this query is used only for generates huge result-set on small data set.
     *
     * @param lazy Lazy mode.
     * @throws Exception On failure.
     */
    public void checkHeavyJoin(boolean lazy) throws Exception {
        try (Connection c = DriverManager.getConnection(
            "jdbc:ignite:thin://127.0.0.1:10800..10850/\"test_cache\"?lazy=" + lazy)) {
            try (Statement stmt = c.createStatement()) {
                log.info("Run heavy join");

                stmt.execute("SELECT * from test T0, test T1, test T2 WHERE T0.id < 200");

                ResultSet rs = stmt.getResultSet();

                long cnt = 0;
                while (rs.next())
                    cnt++;

                log.info("+++ RESULTS " + cnt);
            }
        }
    }



    /** */
    public static class Value {
        /** Secondary ID. */
        @QuerySqlField(index = true)
        private long secId;

        /** String value. */
        @QuerySqlField
        private String str;

        /**
         * @param id ID.
         */
        public Value(long id) {
            secId = secId;
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
