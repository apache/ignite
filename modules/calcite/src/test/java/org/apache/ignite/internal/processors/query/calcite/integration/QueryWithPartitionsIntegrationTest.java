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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import com.google.common.primitives.Ints;
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class QueryWithPartitionsIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final int ENTRIES_COUNT = 10000;

    /** */
    private volatile int[] parts;

    /** */
    @Parameterized.Parameter()
    public boolean local;

    /** */
    @Parameterized.Parameter(1)
    public int partSz;

    /** */
    @Parameterized.Parameters(name = "local = {0}, partSz = {1}")
    public static List<Object[]> parameters() {
        return Stream.of(true, false)
            .flatMap(isLocal -> Stream.of(1, 2, 5, 10, 20)
                .map(i -> new Object[]{isLocal, i}))
            .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        List<Integer> parts0 = IntStream.range(0, 1024).boxed().collect(Collectors.toList());
        Collections.shuffle(parts0);
        parts = Ints.toArray(parts0.subList(0, partSz));

        log.info("Running tests with parts=" + Arrays.toString(parts));
    }

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
        return QueryContext.of(new SqlFieldsQuery("").setLocal(local).setTimeout(10, TimeUnit.SECONDS)
                .setPartitions(parts));
    }

    /** {@inheritDoc} */
    @Override protected List<List<?>> sql(String sql, Object... params) {
        return sql(local ? grid(0) : client, sql, params);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        sql("CREATE TABLE T1(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH cache_name=t1_cache,backups=1");
        sql("CREATE TABLE T2(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH cache_name=t2_cache,backups=1");
        sql("CREATE TABLE DICT(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH template=replicated,cache_name=dict_cache");

        sql("CREATE INDEX T1_IDX ON T1(IDX_VAL)");
        sql("CREATE INDEX T2_IDX ON T2(IDX_VAL)");
        sql("CREATE INDEX DICT_IDX ON DICT(IDX_VAL)");

        Stream.of("T1", "T2", "DICT").forEach(tableName -> {
            StringBuilder sb = new StringBuilder("INSERT INTO ").append(tableName)
                    .append("(ID, IDX_VAL, VAL) VALUES ");

            for (int i = 0; i < ENTRIES_COUNT; ++i) {
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
        Stream.of(Pair.of("SELECT * FROM T1", null),
                  Pair.of("SELECT * FROM T1 WHERE ID < ?", ENTRIES_COUNT))
            .forEach(query -> {
                long cnt = sql(query.left, query.right).size();

                assertEquals(cacheSize("T1_CACHE", parts), cnt);
            });

        Stream.of(Pair.of("SELECT count(*) FROM T1", null),
                  Pair.of("SELECT count(*) FROM T1 WHERE ID < ?", ENTRIES_COUNT))
            .forEach(query -> {
                Long cnt = (Long)sql(query.left, query.right).get(0).get(0);

                assertEquals(cacheSize("T1_CACHE", parts), cnt.longValue());
            });
    }

    /** */
    @Test
    public void testReplicated() {
        Stream.of(Pair.of("select * from DICT", null),
                  Pair.of("select * from DICT where id < ?", ENTRIES_COUNT))
            .forEach(query -> {
                List<List<?>> res = sql(query.left, query.right);

                assertEquals(res.size(), cacheSize("DICT_CACHE"));
            });

        Stream.of(Pair.of("select count(*) from DICT", null),
                  Pair.of("select count(*) from DICT where id < ?", ENTRIES_COUNT))
            .forEach(query -> {
                Long size = (Long)sql(query.left, query.right).get(0).get(0);

                assertEquals(cacheSize("DICT_CACHE"), size.longValue());
            });
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
    private void testJoin(String table1, String table2, String joinCol) {
        String sqlStr = "select * from " + table1 + " join " + table2 +
                " on " + table1 + "." + joinCol + "=" + table2 + "." + joinCol;

        List<?> res = sql(sqlStr);

        assertEquals(res.size(), cacheSize(table1 + "_CACHE", parts));
    }

    /** */
    @Test
    public void testInsertFromSelect() {
        Stream.of(Pair.of("SELECT ID, IDX_VAL, VAL FROM T1 WHERE ID < ?", ENTRIES_COUNT),
                  Pair.of("SELECT ID, IDX_VAL, VAL FROM T1", null))
            .forEach(query -> {
                try {
                    sql("CREATE TABLE T3(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH cache_name=t3_cache,backups=1");

                    sql("INSERT INTO T3(ID, IDX_VAL, VAL) " + query.left, query.right);

                    assertEquals(cacheSize("T1_CACHE", parts), cacheSize("T3_CACHE"));
                }
                finally {
                    client.cache("T3_CACHE").destroy();
                }
            });
    }

    /** */
    @Test
    public void testDelete() {
        Stream.of(Pair.of("DELETE FROM T3 WHERE ID < ?", ENTRIES_COUNT), Pair.of("DELETE FROM T3", null))
            .forEach(query -> {
                try {
                    sql("CREATE TABLE T3(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH cache_name=t3_cache,backups=1");

                    sql("INSERT INTO T3(ID, IDX_VAL, VAL) SELECT ID, IDX_VAL, VAL FROM DICT");

                    assertEquals(ENTRIES_COUNT, cacheFullSize("T3_CACHE"));

                    long partsCnt = cacheSize("T3_CACHE", parts);

                    sql(query.left, query.right);

                    assertEquals(ENTRIES_COUNT - partsCnt, cacheFullSize("T3_CACHE"));
                }
                finally {
                    client.cache("T3_CACHE").destroy();
                }

            });
    }

    /** */
    @Test
    public void testCreateTableAsSelect() {
        Stream.of(Pair.of("SELECT ID, IDX_VAL, VAL FROM T1 WHERE ID < ?", ENTRIES_COUNT),
                  Pair.of("SELECT ID, IDX_VAL, VAL FROM T1", null))
            .forEach(query -> {
                try {
                    sql("CREATE TABLE T3(ID, IDX_VAL, VAL) WITH cache_name=t3_cache,backups=1 AS " + query.left, query.right);

                    assertEquals(cacheSize("T1_CACHE", parts), cacheFullSize("T3_CACHE"));
                }
                finally {
                    client.cache("T3_CACHE").destroy();
                }
            });
    }

    /** */
    private long cacheFullSize(String cacheName) {
        return client.cache(cacheName).sizeLong(CachePeekMode.PRIMARY);
    }

    /** */
    private long cacheSize(String cacheName, int... parts) {
        IgniteCache<?, ?> cache = grid(0).cache(cacheName);

        GridCacheContext<?, ?> ctx = grid(0).cachex(cacheName).context();

        if (F.isEmpty(parts))
            return local && ctx.isPartitioned() ?
                    cache.localSizeLong(CachePeekMode.PRIMARY) : cache.sizeLong(CachePeekMode.PRIMARY);

        return IntStream.of(parts).mapToLong(p -> {
            if (local && ctx.isPartitioned())
                return cache.localSizeLong(p, CachePeekMode.PRIMARY);
            else
                return cache.sizeLong(p, CachePeekMode.PRIMARY);
        }).sum();
    }
}
