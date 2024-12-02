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
 *
 */

package org.apache.ignite.internal.processors.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.H2PooledConnection;
import org.apache.ignite.internal.processors.query.h2.H2QueryInfo;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for lazy mode for DML queries.
 */
@RunWith(Parameterized.class)
public class LazyOnDmlTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 3_000;

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(1)
    public CacheMode cacheMode;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "atomicityMode={0}, cacheMode={1}")
    public static Collection parameters() {
        Set<Object[]> paramsSet = new LinkedHashSet<>();

        Object[] paramTemplate = new Object[2];

        for (CacheAtomicityMode atomicityMode : CacheAtomicityMode.values()) {
            paramTemplate = Arrays.copyOf(paramTemplate, paramTemplate.length);

            paramTemplate[0] = atomicityMode;

            for (CacheMode cacheMode : new CacheMode[] {CacheMode.PARTITIONED, CacheMode.REPLICATED}) {
                Object[] params = Arrays.copyOf(paramTemplate, paramTemplate.length);

                params[1] = cacheMode;

                paramsSet.add(params);
            }
        }

        return paramsSet;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        GridQueryProcessor.idxCls = CheckLazyIndexing.class;

        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteCache<Long, Long> c = grid(0).createCache(new CacheConfiguration<Long, Long>()
            .setName("test")
            .setSqlSchema("TEST")
            .setAtomicityMode(atomicityMode)
            .setCacheMode(cacheMode)
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class.getName(), "testVal")
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val0", Long.class.getName(), null)
                .addQueryField("val1", Long.class.getName(), null)
                .addQueryField("val2", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setIndexes(Collections.singletonList(
                    new QueryIndex(Arrays.asList("val0", "val1"), QueryIndexType.SORTED)
                ))
            ))
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 10)));

        try (IgniteDataStreamer streamer = grid(0).dataStreamer("test")) {
            for (long i = 0; i < KEY_CNT; ++i) {
                BinaryObjectBuilder bob = grid(0).binary().builder("testVal");

                bob.setField("val0", i);
                bob.setField("val1", i);
                bob.setField("val2", i);

                streamer.addData(i, bob.build());
            }
        }

        sql("CREATE TABLE table1 (id INT PRIMARY KEY, col0 INT, col1 VARCHAR (100))");

        sql("INSERT INTO table1 (id, col0, col1) " +
            "SELECT 1, 11, 'FIRST' " +
            "UNION ALL " +
            "SELECT 11,12, 'SECOND' " +
            "UNION ALL " +
            "SELECT 21, 13, 'THIRD' " +
            "UNION ALL " +
            "SELECT 31, 14, 'FOURTH'");

        sql("CREATE TABLE  table2 (id INT PRIMARY KEY, col0 INT, col1 VARCHAR (100))");

        sql("INSERT INTO table2 (id, col0, col1) " +
            "SELECT 1, 21, 'TWO-ONE' " +
            "UNION ALL " +
            "SELECT 11, 22, 'TWO-TWO' " +
            "UNION ALL " +
            "SELECT 21, 23, 'TWO-THREE' " +
            "UNION ALL " +
            "SELECT 31, 24, 'TWO-FOUR'");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (String cache : grid(0).cacheNames())
            grid(0).cache(cache).destroy();

        super.afterTest();
    }

    /**
     */
    @Test
    public void testUpdateNotLazy() throws Exception {
        checkUpdateNotLazy("UPDATE test SET val0 = val0 + 1 WHERE val0 >= 0");
        checkUpdateNotLazy("UPDATE test SET val1 = val1 + 1 WHERE val0 >= 0");
    }

    /**
     */
    public void checkUpdateNotLazy(String sql) throws Exception {
        try (AutoCloseable checker = CheckLazyIndexing.checkLazy(false)) {
            List<List<?>> res = sql(sql).getAll();

            // Check that all rows updates only ones.
            assertEquals((long)KEY_CNT, res.get(0).get(0));
        }
    }

    /**
     */
    @Test
    public void testUpdateLazy() throws Exception {
        checkUpdateLazy("UPDATE test SET val0 = val0 + 1");
        checkUpdateLazy("UPDATE test SET val2 = val2 + 1 WHERE val2 >= 0");
        checkUpdateLazy("UPDATE test SET val0 = val0 + 1 WHERE val1 >= 0");
    }

    /**
     */
    public void checkUpdateLazy(String sql) throws Exception {
        try (AutoCloseable checker = CheckLazyIndexing.checkLazy(true)) {
            List<List<?>> res = sql(sql).getAll();

            // Check that all rows updates only ones.
            assertEquals((long)KEY_CNT, res.get(0).get(0));
        }
    }

    /**
     */
    @Test
    public void testDeleteWithoutReduce() throws Exception {
        try (AutoCloseable checker = CheckLazyIndexing.checkLazy(true)) {
            List<List<?>> res = sql("DELETE FROM test WHERE val0 >= 0").getAll();

            assertEquals((long)KEY_CNT, res.get(0).get(0));
        }
    }

    /**
     */
    @Test
    public void testUpdateFromSubqueryLazy() throws Exception {
        try (AutoCloseable checker = CheckLazyIndexing.checkLazy(true)) {
            List<List<?>> res;

            res = sql("UPDATE table1 " +
                "SET (col0, col1) = " +
                "   (SELECT table2.col0, table2.col1 FROM table2 WHERE table2.id = table1.id)" +
                "WHERE table1.id in (21, 31)").getAll();

            assertEquals(2L, res.get(0).get(0));

            res = sql("UPDATE table1 " +
                "SET (col0, col1) = " +
                "   (SELECT table2.col0, table2.col1 FROM table2 WHERE table2.id = table1.id) " +
                "WHERE exists (select * from table2 where table2.id = table1.id) " +
                "AND table1.id in (21, 31)").getAll();

            assertEquals(2L, res.get(0).get(0));
        }
    }

    /**
     */
    @Test
    public void testUpdateValueField() throws Exception {
        sql("CREATE TABLE TEST2 (id INT PRIMARY KEY, val INT) " +
            "WITH\"WRAP_VALUE=false\"");

        sql("INSERT INTO TEST2 VALUES (0, 0), (1, 1), (2, 2)");

        try (AutoCloseable checker = CheckLazyIndexing.checkLazy(false)) {
            // 'val' field is the alias for _val. There is index for _val.
            List<List<?>> res = sql("UPDATE TEST2 SET _val = _val + 1 WHERE val >=0").getAll();

            assertEquals(3L, res.get(0).get(0));
        }
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return sql(grid(0), sql, args);
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLazy(true)
            .setSchema("TEST")
            .setPageSize(1)
            .setArgs(args), false);
    }

    /** */
    private static class CheckLazyIndexing extends IgniteH2Indexing {
        /** */
        private static Boolean expectedLazy;

        /** */
        private static int qryCnt;

        /** {@inheritDoc} */
        @Override public ResultSet executeSqlQueryWithTimer(PreparedStatement stmt, H2PooledConnection conn, String sql,
            int timeoutMillis, @Nullable GridQueryCancel cancel, Boolean dataPageScanEnabled,
            H2QueryInfo qryInfo) throws IgniteCheckedException {
            if (expectedLazy != null) {
                assertEquals(
                    "Unexpected lazy flag [sql=" + sql + ']',
                    (boolean)expectedLazy,
                    H2Utils.session(conn.connection()).isLazyQueryExecution()
                );
            }

            qryCnt++;

            return super.executeSqlQueryWithTimer(stmt, conn, sql, timeoutMillis, cancel, dataPageScanEnabled, qryInfo);
        }

        /** */
        public static AutoCloseable checkLazy(boolean expLazy) {
            expectedLazy = expLazy;

            return () -> {
                assertTrue("Lazy checker doesn't work properly", CheckLazyIndexing.qryCnt > 0);

                expectedLazy = null;
                qryCnt = 0;
            };
        }
    }
}
