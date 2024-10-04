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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class ViewsIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private boolean persistenceEnabled;

    /** */
    private String[] predefinedSchemas;

    /** */
    @Parameterized.Parameter
    public String engine;

    /** */
    @Parameterized.Parameters(name = "Query engine={0}")
    public static Iterable<Object> params() {
        return Arrays.asList(CalciteQueryEngineConfiguration.ENGINE_NAME, IndexingQueryEngineConfiguration.ENGINE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration()
                .setSqlSchemas(predefinedSchemas)
                .setQueryEnginesConfiguration(engine.equals(CalciteQueryEngineConfiguration.ENGINE_NAME) ?
                    new CalciteQueryEngineConfiguration() : new IndexingQueryEngineConfiguration()))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(persistenceEnabled)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests create and drop view.
     */
    @Test
    public void testCreateDrop() throws Exception {
        initGrids(3);
        initTable(3);

        sql("create or replace view my_view as select * from my_table");

        assertViewsCount("PUBLIC", "MY_VIEW", 1);

        assertRowsCount(3, "select * from my_view");
        assertRowsCount(3, "select t.id, val_int, * from my_view t");

        sql("drop view my_view");

        assertViewsCount("PUBLIC", "MY_VIEW", 0);
    }

    /**
     * Tests views on different schemas.
     */
    @Test
    public void testDifferentSchemas() throws Exception {
        initGrids(3);
        initTable(5);

        IgniteCache<?, ?> cache1 = client.getOrCreateCache(new CacheConfiguration<>("CACHE1")
            .setIndexedTypes(Integer.class, Integer.class));

        IgniteCache<?, ?> cache2 = client.getOrCreateCache(new CacheConfiguration<>("CACHE2")
            .setIndexedTypes(Integer.class, Integer.class));

        IgniteCache<?, ?> cache3 = client.getOrCreateCache(new CacheConfiguration<>("CACHE3")
            .setIndexedTypes(Integer.class, Integer.class)
            .setSqlSchema("MY_SCHEMA")
        );

        IgniteCache<?, ?> cache4 = client.getOrCreateCache(new CacheConfiguration<>("CACHE4")
            .setIndexedTypes(Integer.class, String.class)
            .setSqlSchema("MY_SCHEMA")
        );

        sql("create or replace view my_view as select * from my_table");
        sql("create or replace view cache1.my_view as select * from public.my_table where id < 4");
        sql("create or replace view cache2.my_view as select * from cache1.my_view where id < 3");
        sql("create or replace view my_schema.my_view1 as select * from public.my_table where id < 2");
        sql("create or replace view my_schema.my_view2 as select * from my_view1 where id < 1");

        assertViewsCount("PUBLIC", "MY_VIEW", 1);
        assertViewsCount("CACHE1", "MY_VIEW", 1);
        assertViewsCount("CACHE2", "MY_VIEW", 1);
        assertViewsCount("MY_SCHEMA", "MY_VIEW1", 1);
        assertViewsCount("MY_SCHEMA", "MY_VIEW2", 1);

        assertRowsCount(5, "select * from my_view");
        assertRowsCount(4, "select * from cache1.my_view");
        assertRowsCount(3, "select * from cache2.my_view");
        assertRowsCount(2, "select * from my_schema.my_view1");
        assertRowsCount(1, "select * from my_schema.my_view2");

        // PUBLIC.MY_VIEW
        assertEquals(5, cache1.query(new SqlFieldsQuery("select * from public.my_view")).getAll().size());
        assertEquals(5, cache1.query(new SqlFieldsQuery("select * from my_view").setSchema("PUBLIC")).getAll().size());
        // CACHE1.MY_VIEW
        assertEquals(4, cache1.query(new SqlFieldsQuery("select * from my_view")).getAll().size());
        assertEquals(4, cache2.query(new SqlFieldsQuery("select * from cache1.my_view")).getAll().size());
        // CACHE2.MY_VIEW
        assertEquals(3, cache2.query(new SqlFieldsQuery("select * from my_view")).getAll().size());
        assertEquals(3, cache1.query(new SqlFieldsQuery("select * from my_view").setSchema("CACHE2")).getAll().size());
        // MY_SCHEMA.MY_VIEW1
        assertEquals(2, cache3.query(new SqlFieldsQuery("select * from my_view1")).getAll().size());
        assertEquals(2, cache4.query(new SqlFieldsQuery("select * from my_view1")).getAll().size());
        // MY_SCHEMA.MY_VIEW2
        assertEquals(1, cache3.query(new SqlFieldsQuery("select * from my_view2")).getAll().size());
        assertEquals(1, cache4.query(new SqlFieldsQuery("select * from my_view2")).getAll().size());
    }

    /**
     * Tests views drop on schema drop.
     */
    @Test
    public void testSchemaCleaning() throws Exception {
        initGrids(3);
        initTable(5);

        client.getOrCreateCache(new CacheConfiguration<>("CACHE1")
            .setIndexedTypes(Integer.class, Integer.class));

        client.getOrCreateCache(new CacheConfiguration<>("CACHE2")
            .setIndexedTypes(Integer.class, Integer.class)
            .setSqlSchema("PUBLIC")
        );

        client.getOrCreateCache(new CacheConfiguration<>("CACHE3")
            .setIndexedTypes(Integer.class, Integer.class)
            .setSqlSchema("MY_SCHEMA")
        );

        client.getOrCreateCache(new CacheConfiguration<>("CACHE4")
            .setIndexedTypes(Integer.class, String.class)
            .setSqlSchema("MY_SCHEMA")
        );

        sql("create or replace view my_view as select * from my_table");
        sql("create or replace view cache1.my_view as select * from public.my_table");
        sql("create or replace view my_schema.my_view as select * from public.my_table");

        assertViewsCount("PUBLIC", "MY_VIEW", 1);
        assertViewsCount("CACHE1", "MY_VIEW", 1);
        assertViewsCount("MY_SCHEMA", "MY_VIEW", 1);

        client.destroyCache("CACHE1");

        assertViewsCount("PUBLIC", "MY_VIEW", 1);
        assertViewsCount("CACHE1", "MY_VIEW", 0);
        assertViewsCount("MY_SCHEMA", "MY_VIEW", 1);

        client.destroyCache("CACHE2");

        assertViewsCount("PUBLIC", "MY_VIEW", 1);
        assertViewsCount("CACHE1", "MY_VIEW", 0);
        assertViewsCount("MY_SCHEMA", "MY_VIEW", 1);

        client.destroyCache("CACHE3");

        assertViewsCount("PUBLIC", "MY_VIEW", 1);
        assertViewsCount("CACHE1", "MY_VIEW", 0);
        assertViewsCount("MY_SCHEMA", "MY_VIEW", 1);

        client.destroyCache("CACHE4");

        assertViewsCount("PUBLIC", "MY_VIEW", 1);
        assertViewsCount("CACHE1", "MY_VIEW", 0);
        assertViewsCount("MY_SCHEMA", "MY_VIEW", 0);

        client.getOrCreateCache(new CacheConfiguration<>("CACHE1")
            .setIndexedTypes(Integer.class, Integer.class));

        client.getOrCreateCache(new CacheConfiguration<>("CACHE3")
            .setIndexedTypes(Integer.class, Integer.class)
            .setSqlSchema("MY_SCHEMA")
        );

        assertViewsCount("PUBLIC", "MY_VIEW", 1);
        assertViewsCount("CACHE1", "MY_VIEW", 0);
        assertViewsCount("MY_SCHEMA", "MY_VIEW", 0);
    }

    /**
     * Tests views on not existing schema.
     */
    @Test
    public void testNotExistingSchema() throws Exception {
        initGrids(3);
        initTable(3);

        assertThrows("CREATE VIEW my_schema.my_view AS SELECT * FROM public.my_table", IgniteSQLException.class,
            "MY_SCHEMA");
    }

    /**
     * Tests view after in-memory grid restart.
     */
    @Test
    public void testInMemory() throws Exception {
        initGrids(3);
        initTable(3);

        sql("create or replace view my_view as select * from my_table");

        assertViewsCount("PUBLIC", "MY_VIEW", 1);

        stopAllGrids();

        initGrids(3);

        assertViewsCount("PUBLIC", "MY_VIEW", 0);
    }

    /**
     * Tests view after persistent grid restart.
     */
    @Test
    public void testPersistance() throws Exception {
        persistenceEnabled = true;

        initGrids(3);
        initTable(3);

        sql("create or replace view my_view as select * from my_table");

        assertViewsCount("PUBLIC", "MY_VIEW", 1);

        stopAllGrids();

        initGrids(3);

        assertViewsCount("PUBLIC", "MY_VIEW", 1);
    }

    /**
     * Tests views information distribution on node join.
     */
    @Test
    public void testNodeJoin() throws Exception {
        initGrids(2);
        initTable(3);

        sql("create or replace view my_view as select * from my_table");

        assertViewsCount("PUBLIC", "MY_VIEW", 1);

        startGrid(2);

        assertViewsCount("PUBLIC", "MY_VIEW", 1);

        stopGrid(1);

        sql("create or replace view my_view1 as select * from my_table");

        startGrid(1);

        assertViewsCount("PUBLIC", "MY_VIEW1", 1);
    }

    /**
     * Tests node join with outdated information.
     */
    @Test
    public void testNodeJoinWithDroppedView() throws Exception {
        persistenceEnabled = true;

        initGrids(3);
        initTable(3);

        sql("create or replace view my_view as select * from my_table");

        assertViewsCount("PUBLIC", "MY_VIEW", 1);

        stopGrid(2);

        sql("drop view my_view");

        assertViewsCount("PUBLIC", "MY_VIEW", 0);

        startGrid(2);

        assertViewsCount("PUBLIC", "MY_VIEW", 0);
    }

    /**
     * Tests quoted view and schema names.
     */
    @Test
    public void testQuotedNames() throws Exception {
        String[] quotedNames = new String[] {"\"test\"", "\"test.\"", "\"test.test\"", "\".test\""};

        predefinedSchemas = quotedNames;

        initGrids(3);
        initTable(quotedNames.length * quotedNames.length);

        int cnt = 0;

        for (String schema : quotedNames) {
            for (String view : quotedNames) {
                cnt++;
                String unqoutedSchema = schema.substring(1, schema.length() - 1);
                String unqoutedView = view.substring(1, view.length() - 1);

                sql(String.format("create or replace view %s.%s as select * from public.my_table where id < %d",
                    schema, view, cnt));

                assertViewsCount(unqoutedSchema, unqoutedView, 1);

                assertRowsCount(cnt, String.format("select * from %s.%s", schema, view));
            }
        }

        for (String schema : quotedNames) {
            for (String view : quotedNames) {
                String unqoutedSchema = schema.substring(1, schema.length() - 1);
                String unqoutedView = view.substring(1, view.length() - 1);

                assertViewsCount(unqoutedSchema, unqoutedView, 1);

                sql(String.format("drop view %s.%s", schema, view));

                assertViewsCount(unqoutedSchema, unqoutedView, 0);
            }
        }
    }

    /**
     * Tests create and drop view on SYS schema.
     */
    @Test
    public void testSysSchema() throws Exception {
        String msg = "DDL statements are not supported on SYS schema";

        initGrids(1);

        assertThrows("CREATE OR REPLACE VIEW sys.views AS SELECT * FROM sys.tables", IgniteSQLException.class, msg);
        assertThrows("DROP VIEW IF EXISTS sys.views", IgniteSQLException.class, msg);
    }

    /** */
    private void initGrids(int cnt) throws Exception {
        startGrids(cnt);

        client = startClientGrid();

        client.cluster().state(ClusterState.ACTIVE);
    }

    /** */
    private void initTable(int rows) {
        sql("create table my_table(id int primary key, val_int int, val_str varchar)");

        for (int i = 0; i < rows; i++)
            sql("insert into my_table values (?, ?, ?)", i, i, Integer.toString(i));
    }

    /** */
    @Override protected List<List<?>> sql(IgniteEx ignite, String sql, Object... params) {
        return ignite.context().query().querySqlFields(new SqlFieldsQuery(sql).setArgs(params), true)
            .getAll();
    }

    /** */
    private void assertRowsCount(int cnt, String sql, Object... params) {
        assertEquals(cnt, sql(sql, params).size());
    }

    /** */
    private void assertViewsCount(String schemaName, String viewName, int cnt) {
        for (Ignite ignite : G.allGrids()) {
            try {
                assertEquals(cnt, sql((IgniteEx)ignite, "SELECT * FROM sys.views WHERE schema = ? AND name = ?",
                    schemaName, viewName).size());
            }
            catch (Throwable e) {
                throw new AssertionError("Unexpected views count [grid=" + ignite.name() + ", schemaName=" +
                    schemaName + ", viewName=" + viewName + ']', e);
            }
        }
    }
}
