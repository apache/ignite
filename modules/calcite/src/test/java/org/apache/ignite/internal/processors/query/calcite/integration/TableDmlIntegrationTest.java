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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessorTest;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** */
public class TableDmlIntegrationTest extends GridCommonAbstractTest {
    /** */
    private static final String CLIENT_NODE_NAME = "client";

    /** */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);

        client = startClientGrid(CLIENT_NODE_NAME);
    }

    /** */
    @Before
    public void init() {
        client = grid(CLIENT_NODE_NAME);
    }

    /** */
    @After
    public void cleanUp() {
        client.destroyCaches(client.cacheNames());
    }

    /**
     * Test verifies that already inserted by the current query data
     * is not processed by this query again.
     */
    @Test
    public void testInsertAsSelect() {
        executeSql("CREATE TABLE test (epoch_cur int, epoch_copied int)");
        executeSql("INSERT INTO test VALUES (0, 0)");

        final String insertAsSelectSql = "INSERT INTO test SELECT ?, epoch_cur FROM test";

        for (int i = 1; i < 16; i++) {
            executeSql(insertAsSelectSql, i);

            List<List<?>> rows = executeSql("SELECT * FROM test WHERE epoch_copied = ?", i);

            assertEquals("Unexpected rows for epoch " + i, 0, rows.size());
        }
    }

    /**
     * Test verifies that cuncurrent updates does not affect (in terms of its size)
     * a result set provided for insertion.
     */
    @Test
    public void testInsertAsSelectWithConcurrentDataModification() throws IgniteCheckedException {
        executeSql("CREATE TABLE test (id int primary key, val int) with cache_name=\"test\", value_type=\"my_type\"");
        IgniteCache<Integer, Object> cache = grid(0).cache("test").withKeepBinary();

        BinaryObjectBuilder builder = grid(0).binary().builder("my_type");

        for (int i = 0; i < 128; i++)
            cache.put(i, builder.setField("val", i).build());

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            while (!stop.get())
                cache.put(ThreadLocalRandom.current().nextInt(128), builder.setField("val", 0).build());
        });

        for (int i = 8; i < 18; i++) {
            int off = 1 << (i - 1);

            executeSql("INSERT INTO test SELECT id + ?::INT, val FROM test", off);

            long cnt = (Long)executeSql("SELECT count(*) FROM test").get(0).get(0);

            assertEquals("Unexpected rows count", 1 << i, cnt);
        }

        stop.set(true);
        fut.get(getTestTimeout());
    }

    /**
     * Ensure that update node updates each row only once.
     */
    @Test
    public void testUpdate() {
        executeSql("CREATE TABLE test (val integer)");

        client.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE INDEX test_val_idx ON test (val)").setSchema("PUBLIC"), false).getAll();

        for (int i = 1; i <= 4096; i++)
            executeSql("INSERT INTO test VALUES (?)", i);

        final String updateSql = "UPDATE test SET val = val * 10";

        int mul = 1;
        for (int i = 1; i < 5; i++) {
            mul *= 10;
            executeSql(updateSql);
        }

        final int fMul = mul;

        List<List<?>> rows = executeSql("SELECT val FROM test ORDER BY val");

        List<Integer> vals = rows.stream().map(r -> (Integer)r.get(0)).collect(Collectors.toList());

        for (int rowNum = 1; rowNum <= rows.size(); rowNum++) {
            assertEquals(
                "Unexpected results: " + S.compact(vals, i -> i + fMul),
                rowNum * fMul,
                rows.get(rowNum - 1).get(0)
            );
        }
    }

    /** */
    @Test
    public void testInsertPrimitiveKey() {
        grid(1).getOrCreateCache(new CacheConfiguration<Integer, CalciteQueryProcessorTest.Developer>()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, CalciteQueryProcessorTest.Developer.class)
            .setBackups(2)
        );

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC",
            "INSERT INTO DEVELOPER(_key, name, projectId) VALUES (?, ?, ?)", 0, "Igor", 1);

        assertEquals(1, query.size());

        List<List<?>> rows = query.get(0).getAll();

        assertEquals(1, rows.size());

        List<?> row = rows.get(0);

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        query = engine.query(null, "PUBLIC", "select _key, * from DEVELOPER");

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(Arrays.asList(0, "Igor", 1), row);
    }

    /** */
    @Test
    public void testInsertUpdateDeleteNonPrimitiveKey() throws Exception {
        client.getOrCreateCache(new CacheConfiguration<CalciteQueryProcessorTest.Key, CalciteQueryProcessorTest.Developer>()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(CalciteQueryProcessorTest.Key.class, CalciteQueryProcessorTest.Developer.class)
            .setBackups(2)
        );

        awaitPartitionMapExchange(true, true, null);

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC", "INSERT INTO DEVELOPER VALUES (?, ?, ?, ?)", 0, 0, "Igor", 1);

        assertEquals(1, query.size());

        List<?> row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        query = engine.query(null, "PUBLIC", "select * from DEVELOPER");

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(0, 0, "Igor", 1), row);

        query = engine.query(null, "PUBLIC", "UPDATE DEVELOPER d SET name = name || 'Roman' WHERE id = ?", 0);

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        query = engine.query(null, "PUBLIC", "select * from DEVELOPER");

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(0, 0, "IgorRoman", 1), row);

        query = engine.query(null, "PUBLIC", "DELETE FROM DEVELOPER WHERE id = ?", 0);

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        query = engine.query(null, "PUBLIC", "select * from DEVELOPER");

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNull(row);
    }

    /**
     * Test verifies that a type inference works properly in case of
     * multiple values list consisting of least restrictive type
     * comparing to table field type and NULL literal.
     */
    @Test
    public void testInsertMultipleValuesWithNullAndImplicitTypeConversion() {
        executeSql("CREATE TABLE test (d DATE)");
        executeSql("INSERT INTO test VALUES ('2000-10-10'), (null)");

        assertEquals(2L, executeSql("select count(*) from test").get(0).get(0));
    }

    /** */
    private List<List<?>> executeSql(String sql, Object... args) {
        List<FieldsQueryCursor<List<?>>> cur = queryProcessor().query(null, "PUBLIC", sql, args);

        try (QueryCursor<List<?>> srvCursor = cur.get(0)) {
            return srvCursor.getAll();
        }
    }

    /** */
    private CalciteQueryProcessor queryProcessor() {
        return Commons.lookupComponent(client.context(), CalciteQueryProcessor.class);
    }
}
