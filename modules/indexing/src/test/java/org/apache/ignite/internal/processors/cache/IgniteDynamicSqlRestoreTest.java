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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 */
@SuppressWarnings("Duplicates")
public class IgniteDynamicSqlRestoreTest extends GridCommonAbstractTest implements Serializable {

    public static final String TEST_CACHE_NAME = "test";

    public static final String TEST_INDEX_OBJECT = "TestIndexObject";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setAutoActivationEnabled(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(200 * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testMergeChangedConfigOnCoordinator() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            IgniteCache cache = ig.getOrCreateCache(getTestTableConfiguration());

            fillTestData(ig);

            //when: stop one node and create indexes on other node
            stopGrid(1);

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexb on TestIndexObject(b)")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject add column (c int)")).getAll();

            //and: stop all grid
            stopAllGrids();
        }

        {
            //and: start cluster from node without index
            IgniteEx ig = startGrid(1);
            startGrid(0);

            ig.cluster().active(true);

            //and: change data
            try (IgniteDataStreamer<Object, Object> s = ig.dataStreamer(TEST_CACHE_NAME)) {
                s.allowOverwrite(true);
                for (int i = 0; i < 50; i++)
                    s.addData(i, null);
            }

            stopAllGrids();
        }

        {
            //when: start node from first node
            IgniteEx ig0 = startGrid(0);
            IgniteEx ig1 = startGrid(1);

            ig0.cluster().active(true);

            //then: everything is ok
            try (IgniteDataStreamer<Object, Object> s = ig1.dataStreamer(TEST_CACHE_NAME)) {
                s.allowOverwrite(true);
                for (int i = 0; i < 50; i++) {
                    BinaryObject bo = ig1.binary().builder(TEST_INDEX_OBJECT)
                        .setField("a", i, Object.class)
                        .setField("b", String.valueOf(i), Object.class)
                        .setField("c", i, Object.class)
                        .build();

                    s.addData(i, bo);
                }
            }

            IgniteCache<Object, Object> cache = ig1.cache(TEST_CACHE_NAME);

            assertIndexUsed(cache, "explain select * from TestIndexObject where a > 5", "myindexa");
            assertFalse(cache.query(new SqlFieldsQuery("SELECT a,b,c FROM TestIndexObject limit 1")).getAll().isEmpty());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("AssertWithSideEffects")
    @Test
    public void testIndexCreationWhenNodeStopped() throws Exception {
        // Start topology.
        startGrid(0);
        Ignite srv2 = startGrid(1);
        Ignite cli = startClientGrid(2);

        cli.cluster().active(true);

        // Create table, add some data.
        int entryCnt = 50;

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10802")) {
            executeJdbc(conn,
                " CREATE TABLE PERSON (\n" +
                " FIRST_NAME VARCHAR,\n" +
                " LAST_NAME VARCHAR,\n" +
                " ADDRESS VARCHAR,\n" +
                " LANG VARCHAR,\n" +
                " BIRTH_DATE TIMESTAMP,\n" +
                " CONSTRAINT PK_PERSON PRIMARY KEY (FIRST_NAME,LAST_NAME,ADDRESS,LANG)\n" +
                " ) WITH \"key_type=PersonKeyType, CACHE_NAME=PersonCache, value_type=PersonValueType, AFFINITY_KEY=FIRST_NAME,template=PARTITIONED,backups=1\"");

            try (PreparedStatement stmt = conn.prepareStatement(
                "insert into Person(LANG, FIRST_NAME, ADDRESS, LAST_NAME, BIRTH_DATE) values(?,?,?,?,?)")) {
                for (int i = 0; i < entryCnt; i++) {
                    String s = String.valueOf(i);

                    stmt.setString(1, s);
                    stmt.setString(2, s);
                    stmt.setString(3, s);
                    stmt.setString(4, s);
                    stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));

                    stmt.executeUpdate();
                }
            }
        }

        // Stop second node.
        srv2.close();

        // Create an index on remaining node.
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10802")) {
            executeJdbc(conn, "create index PERSON_FIRST_NAME_IDX on PERSON(FIRST_NAME)");
        }

        // Restart second node.
        startGrid(1);

        // Await for index rebuild on started node.
        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10801")) {
                    try (PreparedStatement stmt = conn.prepareStatement(
                        "EXPLAIN SELECT * FROM Person USE INDEX(PERSON_FIRST_NAME_IDX) WHERE FIRST_NAME=?")) {
                        stmt.setString(1, String.valueOf(1));

                        StringBuilder fullPlan = new StringBuilder();

                        try (ResultSet rs = stmt.executeQuery()) {
                            while (rs.next())
                                fullPlan.append(rs.getString(1)).append("; ");
                        }

                        System.out.println("PLAN: " + fullPlan);

                        return fullPlan.toString().contains("PUBLIC.PERSON_FIRST_NAME_IDX");
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException("Query failed.", e);
                }
            }
        }, 5_000);

        // Make sure that data could be queried.
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10802")) {
            try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT COUNT(*) FROM Person USE INDEX(PERSON_FIRST_NAME_IDX) WHERE FIRST_NAME=?")) {
                for (int i = 0; i < entryCnt; i++) {
                    stmt.setString(1, String.valueOf(i));

                    try (ResultSet rs = stmt.executeQuery()) {
                        rs.next();

                        long cnt = rs.getLong(1);

                        assertEquals(1L, cnt);
                    }
                }
            }
        }
    }

    /**
     * Execute a statement through JDBC connection.
     *
     * @param conn Connection.
     * @param sql Statement.
     * @throws Exception If failed.
     */
    private static void executeJdbc(Connection conn, String sql) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTakeConfigFromJoiningNodeOnInactiveGrid() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            IgniteCache cache = ig.getOrCreateCache(getTestTableConfiguration());

            fillTestData(ig);

            stopGrid(1);

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexb on TestIndexObject(b)")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject add column (c int)")).getAll();

            stopAllGrids();
        }

        {
            //and: start cluster from node without cache
            IgniteEx ig = startGrid(1);
            startGrid(0);

            ig.cluster().active(true);

            //then: config for cache was applying successful
            IgniteCache<Object, Object> cache = ig.cache(TEST_CACHE_NAME);

            assertIndexUsed(cache, "explain select * from TestIndexObject where a > 5", "myindexa");
            assertFalse(cache.query(new SqlFieldsQuery("SELECT a,b,c FROM TestIndexObject limit 1")).getAll().isEmpty());
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testResaveConfigAfterMerge() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            IgniteCache cache = ig.getOrCreateCache(getTestTableConfiguration());

            fillTestData(ig);

            stopGrid(1);

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexb on TestIndexObject(b)")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject add column (c int)")).getAll();

            stopAllGrids();
        }

        {
            //when: start cluster from node without cache
            IgniteEx ig = startGrid(1);
            startGrid(0);

            ig.cluster().active(true);

            stopAllGrids();
        }

        {
            //then: start only one node which originally was without index
            IgniteEx ig = startGrid(1);

            ig.cluster().active(true);
            resetBaselineTopology();
            ig.resetLostPartitions(Collections.singleton(TEST_CACHE_NAME));

            IgniteCache<Object, Object> cache = ig.cache(TEST_CACHE_NAME);

            assertIndexUsed(cache, "explain select * from TestIndexObject where a > 5", "myindexa");
            assertFalse(cache.query(new SqlFieldsQuery("SELECT a,b,c FROM TestIndexObject limit 1")).getAll().isEmpty());
        }
    }

    /**
     * @throws Exception if failed.
     */
    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Test
    public void testMergeChangedConfigOnInactiveGrid() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);
            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.put("A", "java.lang.Integer");
            fields.put("B", "java.lang.String");

            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(TEST_CACHE_NAME);

            ccfg.setQueryEntities(Arrays.asList(
                new QueryEntity()
                    .setKeyType("java.lang.Integer")
                    .setValueType("TestIndexObject")
                    .setFields(fields)
            ));

            IgniteCache cache = ig.getOrCreateCache(ccfg);

            fillTestData(ig);

            cache.query(new SqlFieldsQuery("create index myindexb on TestIndexObject(b)")).getAll();

            //and: stop one node and create index on other node
            stopGrid(1);

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();
            cache.query(new SqlFieldsQuery("drop index myindexb")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject drop column b")).getAll();

            //and: stop all grid
            stopAllGrids();
        }

        {
            //and: start cluster
            IgniteEx ig0 = startGrid(0);
            IgniteEx ig1 = startGrid(1);

            ig0.cluster().active(true);

            //then: config should be merged
            try (IgniteDataStreamer<Object, Object> s = ig1.dataStreamer(TEST_CACHE_NAME)) {
                s.allowOverwrite(true);
                for (int i = 0; i < 50; i++) {
                    BinaryObject bo = ig1.binary().builder("TestIndexObject")
                        .setField("a", i, Object.class)
                        .setField("b", String.valueOf(i), Object.class)
                        .build();

                    s.addData(i, bo);
                }
            }
            IgniteCache<Object, Object> cache = ig1.cache(TEST_CACHE_NAME);

            //then: index "myindexa" and column "b" restored from node "1"
            assertIndexUsed(cache, "explain select * from TestIndexObject where a > 5", "myindexa");
            assertIndexUsed(cache, "explain select * from TestIndexObject where b > 5", "myindexb");
            assertFalse(cache.query(new SqlFieldsQuery("SELECT a,b FROM TestIndexObject limit 1")).getAll().isEmpty());
        }
    }

    /**
     * Make sure that index is used for the given statement.
     *
     * @param cache Cache.
     * @param sql Statement.
     * @param idx Index.
     * @throws IgniteCheckedException If failed.
     */
    private void assertIndexUsed(IgniteCache<Object, Object> cache, String sql, String idx)
        throws IgniteCheckedException {
        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                String plan = doExplainPlan(cache, sql);

                return plan.contains(idx);
            }
        }, 10_000);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTakeChangedConfigOnActiveGrid() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            IgniteCache cache = ig.getOrCreateCache(getTestTableConfiguration());

            fillTestData(ig);

            //stop one node and create index on other node
            stopGrid(1);

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexb on TestIndexObject(b)")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject add column (c int)")).getAll();

            stopAllGrids();
        }

        {
            //and: start cluster
            IgniteEx ig = startGrid(0);
            ig.cluster().active(true);

            ig = startGrid(1);
            ig.resetLostPartitions(Collections.singleton(TEST_CACHE_NAME));

            //then: config should be merged
            try (IgniteDataStreamer<Object, Object> s = ig.dataStreamer(TEST_CACHE_NAME)) {
                s.allowOverwrite(true);
                for (int i = 0; i < 50; i++) {
                    BinaryObject bo = ig.binary().builder("TestIndexObject")
                        .setField("a", i, Object.class)
                        .setField("b", String.valueOf(i), Object.class)
                        .setField("c", i, Object.class)
                        .build();

                    s.addData(i, bo);
                }
            }
            IgniteCache<Object, Object> cache = ig.getOrCreateCache(TEST_CACHE_NAME);

            cache.indexReadyFuture().get();

            assertIndexUsed(cache, "explain select * from TestIndexObject where a > 5", "myindexa");
            assertFalse(cache.query(new SqlFieldsQuery("SELECT a,b,c FROM TestIndexObject limit 1")).getAll().isEmpty());
        }
    }

    /**
     * @throws Exception if failed.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFailJoiningNodeBecauseDifferentSql() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            IgniteCache cache = ig.getOrCreateCache(getTestTableConfiguration());

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();

            //stop one node and create index on other node
            stopGrid(1);

            cache.query(new SqlFieldsQuery("drop index myindexa")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject drop column b")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject add column (b int)")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(b)")).getAll();

            //and: stopped all grid
            stopAllGrids();
        }

        {
            //and: start cluster
            startGrid(0);
            try {
                startGrid(1);

                fail("Node should start with fail");
            }
            catch (Exception e) {
                String cause = X.cause(e, IgniteSpiException.class).getMessage();
                assertThat(cause, containsString("fieldType of B is different"));
                assertThat(cause, containsString("index MYINDEXA is different"));
            }
        }

    }

    /**
     * @throws Exception if failed.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFailJoiningNodeBecauseFieldInlineSizeIsDifferent() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            IgniteCache cache = ig.getOrCreateCache(getTestTableConfiguration());

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a) INLINE_SIZE 100")).getAll();

            //stop one node and create index on other node
            stopGrid(1);

            cache.query(new SqlFieldsQuery("drop index myindexa")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a) INLINE_SIZE 200")).getAll();

            //and: stopped all grid
            stopAllGrids();
        }

        {
            //and: start cluster
            startGrid(0);
            try {
                startGrid(1);

                fail("Node should start with fail");
            }
            catch (Exception e) {
                assertThat(X.cause(e, IgniteSpiException.class).getMessage(), containsString("index MYINDEXA is different"));
            }
        }

    }

    /**
     * @throws Exception if failed.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFailJoiningNodeBecauseNeedConfigUpdateOnActiveGrid() throws Exception {
        {
            startGrid(0);
            startGrid(1);

            CacheConfiguration<Object, Object> ccfg = getTestTableConfiguration();

            Ignite ig = ignite(0);

            ig.cluster().active(true);

            IgniteCache cache = ig.getOrCreateCache(ccfg);

            fillTestData(ig);

            stopGrid(1);

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexb on TestIndexObject(b)")).getAll();

            stopGrid(0);
        }

        {
            IgniteEx ig = startGrid(1);
            ig.cluster().active(true);

            try {
                startGrid(0);

                fail("Node should start with fail");
            }
            catch (Exception e) {
                assertThat(X.cause(e, IgniteSpiException.class).getMessage(), containsString("Failed to join node to the active cluster"));
            }
        }
    }

    /**
     * @return result of explain plan
     */
    @NotNull private String doExplainPlan(IgniteCache<Object, Object> cache, String sql) {
        return cache.query(new SqlFieldsQuery(sql)).getAll().get(0).get(0).toString().toLowerCase();
    }

    /**
     * fill data by default
     */
    private void fillTestData(Ignite ig) {
        try (IgniteDataStreamer<Object, Object> s = ig.dataStreamer(TEST_CACHE_NAME)) {
            for (int i = 0; i < 500; i++) {
                BinaryObject bo = ig.binary().builder("TestIndexObject")
                    .setField("a", i, Object.class)
                    .setField("b", String.valueOf(i), Object.class)
                    .build();

                s.addData(i, bo);
            }
        }
    }

    /**
     * @return cache configuration with test table
     */
    @NotNull private CacheConfiguration<Object, Object> getTestTableConfiguration() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("a", "java.lang.Integer");
        fields.put("B", "java.lang.String");

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(TEST_CACHE_NAME);

        ccfg.setQueryEntities(Collections.singletonList(
            new QueryEntity()
                .setKeyType("java.lang.Integer")
                .setValueType("TestIndexObject")
                .setFields(fields)
        ));
        return ccfg;
    }
}
