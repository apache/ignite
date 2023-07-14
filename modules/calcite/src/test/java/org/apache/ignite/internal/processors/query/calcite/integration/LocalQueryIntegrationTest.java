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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.junit.Test;

/** */
public class LocalQueryIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final int ENTRIES_COUNT = 10000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected QueryChecker assertQuery(String qry) {
        return assertQuery(grid(0), qry);
    }

    /** {@inheritDoc} */
    @Override protected QueryContext queryContext() {
        return QueryContext.of(new SqlFieldsQuery("").setLocal(true));
    }

    /** {@inheritDoc} */
    @Override protected List<List<?>> sql(String sql, Object... params) {
        return sql(grid(0), sql, params);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        sql("CREATE TABLE T1(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH cache_name=t1_cache");
        sql("CREATE TABLE T2(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH cache_name=t2_cache");
        sql("CREATE TABLE DICT(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH template=replicated,cache_name=dict_cache");

        sql("CREATE INDEX T1_IDX ON T1(IDX_VAL)");
        sql("CREATE INDEX T2_IDX ON T2(IDX_VAL)");
        sql("CREATE INDEX DICT_IDX ON DICT(IDX_VAL)");

        Stream.of("T1", "T2", "DICT").forEach(tableName -> {
            StringBuilder sb = new StringBuilder("INSERT INTO ").append(tableName)
                    .append("(ID, IDX_VAL, VAL) VALUES ");

            for (int i = 0; i < 10000; ++i) {
                sb.append("(").append(i).append(", ")
                        .append("'name_").append(i).append("', ")
                        .append("'name_").append(i).append("')");

                if (i < ENTRIES_COUNT - 1)
                    sb.append(",");
            }

            sql(sb.toString());

            assertEquals(ENTRIES_COUNT, client.getOrCreateCache(tableName + "_CACHE").size(CachePeekMode.PRIMARY));
        });
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        // Skip super method to keep caches after each test.
    }

    /** */
    @Test
    public void testSingle() {
        test("select * from T1 order by id", "T1_CACHE");
    }

    /** */
    @Test
    public void testReplicated() {
        List<List<?>> res = sql("select * from DICT");

        assertEquals(grid(0).cache("DICT_CACHE").size(CachePeekMode.PRIMARY), res.size());
    }

    /** */
    @Test
    public void testJoin() {
        Stream.of("ID", "IDX_VAL", "VAL").forEach(col -> testJoin("T1", "T2", col));
    }

    /** */
    @Test
    public void testJoinReplicated() {
        Stream.of("ID", "IDX_VAL", "VAL").forEach(col -> testJoin("T1", "DICT", col));
    }

    /** */
    @Test
    public void testInsertFromSelect() {
        try {
            sql("CREATE TABLE T3(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH cache_name=t3_cache");

            sql("INSERT INTO T3(ID, IDX_VAL, VAL) SELECT ID, IDX_VAL, VAL FROM T1 WHERE ID < ?", ENTRIES_COUNT);

            assertEquals(grid(0).cache("T1_CACHE").localSizeLong(CachePeekMode.PRIMARY),
                    client.cache("T3_CACHE").sizeLong(CachePeekMode.PRIMARY));
        }
        finally {
            grid(0).cache("T3_CACHE").destroy();
        }
    }

    /** */
    @Test
    public void testDelete() {
        try {
            sql("CREATE TABLE T3(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH cache_name=t3_cache");

            sql("INSERT INTO T3(ID, IDX_VAL, VAL) SELECT ID, IDX_VAL, VAL FROM DICT");

            assertEquals(ENTRIES_COUNT, client.cache("T3_CACHE").sizeLong(CachePeekMode.PRIMARY));

            long localSize = grid(0).cache("T3_CACHE").localSizeLong(CachePeekMode.PRIMARY);

            sql("DELETE FROM T3 WHERE ID < ?", ENTRIES_COUNT);

            assertEquals(ENTRIES_COUNT - localSize, client.cache("T3_CACHE").sizeLong(CachePeekMode.PRIMARY));
        }
        finally {
            grid(0).cache("T3_CACHE").destroy();
        }
    }

    /** */
    @Test
    public void testCreateTableAsSelect() {
        try {
            sql("CREATE TABLE T3(ID, IDX_VAL, VAL) WITH cache_name=t3_cache AS SELECT ID, IDX_VAL, VAL FROM T1");

            assertEquals(grid(0).cache("T1_CACHE").localSizeLong(CachePeekMode.PRIMARY),
                    client.cache("T3_CACHE").sizeLong(CachePeekMode.PRIMARY));
        }
        finally {
            grid(0).cache("T3_CACHE").destroy();
        }
    }

    /** */
    private void testJoin(String table1, String table2, String joinCol) {
        String sql = "select * from " + table1 + " join " + table2 +
                        " on " + table1 + "." + joinCol + "=" + table2 + "." + joinCol;

        test(sql, table1 + "_CACHE");
    }

    /** */
    private void test(String sql, String cacheName) {
        List<List<?>> res = sql(sql);

        Affinity<Object> aff = grid(0).affinity(cacheName);
        ClusterNode localNode = grid(0).localNode();

        List<?> primaries = res.stream().filter(l -> {
                int part = aff.partition(l.get(0));

                return aff.isPrimary(localNode, part);
            }
        ).collect(Collectors.toList());;

        assertEquals(primaries.size(), res.size());
    }
}
