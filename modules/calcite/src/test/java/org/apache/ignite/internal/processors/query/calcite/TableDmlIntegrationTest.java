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
package org.apache.ignite.internal.processors.query.calcite;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

/** */
public class TableDmlIntegrationTest extends GridCommonAbstractTest {
    /** */
    private static final String CLIENT_NODE_NAME = "client";

    /** */
    private static final String DATA_REGION_NAME = "test_data_region";

    /** */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);

        client = startClientGrid(CLIENT_NODE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(
                new SqlConfiguration().setSqlSchemas("MY_SCHEMA")
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDataRegionConfigurations(new DataRegionConfiguration().setName(DATA_REGION_NAME))
            );
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
        // current implementation of DISABLE_RULE hint is not working
        // for SELECT that is not a query root, but let's write this
        // test in the way it would be splitted later to verify both
        // Index and Table scan after aforemention limitation will be fixed
        final String insertAsSelectSql = "INSERT INTO test SELECT ?, epoch_cur FROM test";
        final String notExpScan = "IgniteTableScan";

        executeSql("CREATE TABLE test (epoch_cur int, epoch_copied int)");
        executeSql("INSERT INTO test VALUES (0, 0)");

        assertThat(
            (String)executeSql("explain plan for " + insertAsSelectSql, 1).get(0).get(0),
            not(containsString(notExpScan))
        );

        for (int i = 1; i < 16; i++) {
            executeSql(insertAsSelectSql, i);

            List<List<?>> rows = executeSql("SELECT * FROM test WHERE epoch_copied = ?", i);

            assertEquals("Unexpected rows for epoch " + i, 0, rows.size());
        }
    }

    /** */
    @Test
    public void testInsertPrimitiveKey() throws Exception {
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

        query = engine.query(null, "PUBLIC", "UPDATE DEVELOPER d SET name = 'Roman' WHERE id = ?", 0);

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(1L), row);

        query = engine.query(null, "PUBLIC", "select * from DEVELOPER");

        assertEquals(1, query.size());

        row = F.first(query.get(0).getAll());

        assertNotNull(row);

        assertEqualsCollections(F.asList(0, 0, "Roman", 1), row);

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
