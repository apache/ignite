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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.TestUtils.hasSize;
import static org.apache.ignite.internal.util.IgniteUtils.map;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/** */
public class TableDdlIntegrationTest extends AbstractDdlIntegrationTest {
    /**
     * Creates table with two columns, where the first column is PK,
     * and verifies created cache.
     */
    @Test
    public void createTableSimpleCase() {
        Set<String> cachesBefore = new HashSet<>(client.cacheNames());

        sql("create table my_table (id int primary key, val varchar)");

        Set<String> newCaches = new HashSet<>(client.cacheNames());
        newCaches.removeAll(cachesBefore);

        assertThat(newCaches, hasSize(1));

        CacheConfiguration<?, ?> ccfg = client.cachex(newCaches.iterator().next()).configuration();

        assertThat(ccfg.getQueryEntities(), hasSize(1));

        QueryEntity ent = ccfg.getQueryEntities().iterator().next();

        assertThat(ent.getTableName(), equalTo("MY_TABLE"));
        assertThat(ent.getKeyFieldName(), equalTo("ID"));
        assertThat(
            ent.getFields(),
            equalTo(new LinkedHashMap<>(
                map(
                    "ID", Integer.class.getName(),
                    "VAL", String.class.getName()
                )
            ))
        );
    }

    /**
     * Creates table with columns of different data types and check DML on these columns.
     */
    @Test
    public void createTableDifferentDataTypes() {
        sql("create table my_table (" +
            "id int primary key, " +
            "c1 varchar, " +
            "c2 date, " +
            "c3 time, " +
            "c4 timestamp, " +
            "c5 integer, " +
            "c6 bigint, " +
            "c7 smallint, " +
            "c8 tinyint, " +
            "c9 boolean, " +
            "c10 double, " +
            "c11 float, " +
            "c12 decimal(20, 10) " +
            ")");

        sql("insert into my_table values (" +
            "0, " +
            "'test', " +
            "date '2021-01-01', " +
            "time '12:34:56', " +
            "timestamp '2021-01-01 12:34:56', " +
            "1, " +
            "9876543210, " +
            "3, " +
            "4, " +
            "true, " +
            "123.456, " +
            "654.321, " +
            "1234567890.1234567890" +
            ")");

        List<List<?>> res = sql("select * from my_table");

        assertEquals(1, res.size());

        List<?> row = res.get(0);

        assertEquals(0, row.get(0));
        assertEquals("test", row.get(1));
        assertEquals(Date.valueOf("2021-01-01"), row.get(2));
        assertEquals(Time.valueOf("12:34:56"), row.get(3));
        assertEquals(Timestamp.valueOf("2021-01-01 12:34:56"), row.get(4));
        assertEquals(1, row.get(5));
        assertEquals(9876543210L, row.get(6));
        assertEquals((short)3, row.get(7));
        assertEquals((byte)4, row.get(8));
        assertEquals(Boolean.TRUE, row.get(9));
        assertEquals(123.456d, row.get(10));
        assertEquals(654.321f, row.get(11));
        assertEquals(new BigDecimal("1234567890.1234567890"), row.get(12));
    }

    /**
     * Creates table with all possible options except template,
     * and verifies created cache.
     */
    @Test
    public void createTableWithOptions() {
        Set<String> cachesBefore = new HashSet<>(client.cacheNames());

        sql("create table my_table (id1 int, id2 int, val varchar, primary key(id1, id2)) with " +
            " backups=2," +
            " affinity_key=id2," +
            " atomicity=transactional," +
            " write_synchronization_mode=full_async," +
            " cache_group=my_cache_group," +
            " cache_name=my_cache_name," +
            " data_region=\"" + DATA_REGION_NAME + "\"," +
            " key_type=my_key_type," +
            " value_type=my_value_type"
// due to uncertain reason a test hangs if encryption is enabled
//            " encrypted=true"
        );

        Set<String> newCaches = new HashSet<>(client.cacheNames());
        newCaches.removeAll(cachesBefore);

        assertThat(newCaches, hasSize(1));

        CacheConfiguration<?, ?> ccfg = client.cachex(newCaches.iterator().next()).configuration();

        assertThat(ccfg.getQueryEntities(), hasSize(1));
        assertThat(ccfg.getBackups(), equalTo(2));
        assertThat(ccfg.getAtomicityMode(), equalTo(CacheAtomicityMode.TRANSACTIONAL));
        assertThat(ccfg.getWriteSynchronizationMode(), equalTo(CacheWriteSynchronizationMode.FULL_ASYNC));
        assertThat(ccfg.getGroupName(), equalTo("MY_CACHE_GROUP"));
        assertThat(ccfg.getName(), equalTo("MY_CACHE_NAME"));
        assertThat(ccfg.getDataRegionName(), equalTo(DATA_REGION_NAME));
        assertThat(ccfg.getKeyConfiguration()[0].getAffinityKeyFieldName(), equalTo("ID2"));

        QueryEntity ent = ccfg.getQueryEntities().iterator().next();

        assertThat(ent.getTableName(), equalTo("MY_TABLE"));
        assertThat(ent.getKeyType(), equalTo("MY_KEY_TYPE"));
        assertThat(ent.getValueType(), equalTo("MY_VALUE_TYPE"));
        assertThat(ent.getKeyFields(), equalTo(new LinkedHashSet<>(F.asList("ID1", "ID2"))));
    }

    /**
     * Creates table with all possible options (as double-quoted identifier) except template,
     * and verifies created cache.
     */
    @Test
    public void createTableWithOptionsQuoted() {
        Set<String> cachesBefore = new HashSet<>(client.cacheNames());

        sql("create table my_table (id1 int, id2 int, val varchar, primary key(id1, id2)) with \"" +
                " backups=2," +
                " affinity_key=id2," +
                " atomicity=transactional," +
                " write_synchronization_mode=full_async," +
                " cache_group=my_cache_group," +
                " cache_name=my_cache_name," +
                " data_region=" + DATA_REGION_NAME + "," +
                " key_type=my_key_type," +
                " value_type=my_value_type" +
// due to uncertain reason a test hangs if encryption is enabled
//                " encrypted=true" +
                "\""
        );

        Set<String> newCaches = new HashSet<>(client.cacheNames());
        newCaches.removeAll(cachesBefore);

        assertThat(newCaches, hasSize(1));

        CacheConfiguration<?, ?> ccfg = client.cachex(newCaches.iterator().next()).configuration();

        assertThat(ccfg.getQueryEntities(), hasSize(1));
        assertThat(ccfg.getBackups(), equalTo(2));
        assertThat(ccfg.getAtomicityMode(), equalTo(CacheAtomicityMode.TRANSACTIONAL));
        assertThat(ccfg.getWriteSynchronizationMode(), equalTo(CacheWriteSynchronizationMode.FULL_ASYNC));
        assertThat(ccfg.getGroupName(), equalTo("my_cache_group"));
        assertThat(ccfg.getName(), equalTo("my_cache_name"));
        assertThat(ccfg.getDataRegionName(), equalTo(DATA_REGION_NAME));
        assertThat(ccfg.getKeyConfiguration()[0].getAffinityKeyFieldName(), equalTo("id2"));

        QueryEntity ent = ccfg.getQueryEntities().iterator().next();

        assertThat(ent.getTableName(), equalTo("MY_TABLE"));
        assertThat(ent.getKeyType(), equalTo("my_key_type"));
        assertThat(ent.getValueType(), equalTo("my_value_type"));
        assertThat(ent.getKeyFields(), equalTo(new LinkedHashSet<>(F.asList("ID1", "ID2"))));
    }

    /**
     * Creates several tables with specified templates,
     * and verifies created caches.
     */
    @Test
    @SuppressWarnings("rawtypes")
    public void createTableWithTemplate() {
        Set<String> cachesBefore = new HashSet<>(client.cacheNames());

        sql("create table my_table_replicated (id int, val varchar) with template=replicated, cache_name=repl");
        sql("create table my_table_partitioned (id int, val varchar) with template=partitioned, cache_name=part");

        Set<String> newCaches = new HashSet<>(client.cacheNames());
        newCaches.removeAll(cachesBefore);

        assertThat(newCaches, hasSize(2));

        List<CacheConfiguration> ccfgs = newCaches.stream()
            .map(client::cachex)
            .map(IgniteInternalCache::configuration)
            .collect(Collectors.toList());

        assertThat(ccfgs, hasItem(matches("replicated cache",
            ccfg -> "REPL".equals(ccfg.getName()) && ccfg.getCacheMode() == CacheMode.REPLICATED)));
        assertThat(ccfgs, hasItem(matches("partitioned cache",
            ccfg -> "PART".equals(ccfg.getName()) && ccfg.getCacheMode() == CacheMode.PARTITIONED)));
    }

    /**
     * Tries to create several tables with the same name.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void createTableIfNotExists() {
        sql("create table my_table (id int, val varchar)");

        GridTestUtils.assertThrows(log,
            () -> sql("create table my_table (id int, val varchar)"),
            IgniteSQLException.class, "Table already exists: MY_TABLE");

        sql("create table if not exists my_table (id int, val varchar)");
    }

    /**
     * Create table using reserved word
     */
    @Test
    public void createTableUseReservedWord() {
        assertThrows("create table table (id int primary key, val varchar)", IgniteSQLException.class,
            "Failed to parse query. Encountered \"table table\"");

        sql("create table \"table\" (id int primary key, val varchar)");

        sql("insert into \"table\" (id, val) values (0, '1')");

        assertQuery("select * from \"table\" ").returns(0, "1").check();
    }

    /**
     * Creates a table without a primary key and then insert a few rows.
     */
    @Test
    public void createTableWithoutPk() {
        sql("create table my_table (f1 int, f2 varchar)");

        sql("insert into my_table(f1, f2) values (1, '1'),(2, '2')");
        sql("insert into my_table values (1, '1'),(2, '2')");

        assertThat(sql("select * from my_table"), hasSize(4));
    }

    /**
     * Creates a table in a custom schema.
     */
    @Test
    public void createTableCustomSchema() {
        sql("create table my_schema.my_table (f1 int, f2 varchar)");

        sql("insert into my_schema.my_table(f1, f2) values (1, '1'),(2, '2')");
        sql("insert into my_schema.my_table values (1, '1'),(2, '2')");

        assertThat(sql("select * from my_schema.my_table"), hasSize(4));
    }

    /**
     * Creates a table for already existing cache.
     */
    @Test
    public void createTableOnExistingCache() {
        IgniteCache<Object, Object> cache = client.getOrCreateCache("my_cache");

        sql("create table my_schema.my_table (f1 int, f2 varchar) with cache_name=\"my_cache\"");

        sql("insert into my_schema.my_table(f1, f2) values (1, '1'),(2, '2')");

        assertThat(sql("select * from my_schema.my_table"), hasSize(2));

        assertEquals(2, cache.size(CachePeekMode.PRIMARY));
    }

    /**
     * Creates table as select.
     */
    @Test
    public void createTableAsSelectSimpleCase() {
        sql("create table my_table as select 1 as i, 'test' as s");
        List<List<?>> res = sql("select i, s from my_table");

        assertEquals(1, res.size());

        List<?> row = res.get(0);

        assertEquals(1, row.get(0));
        assertEquals("test", row.get(1));
    }

    /**
     * Creates table with specified columns as select.
     */
    @Test
    public void createTableAsSelectWithColumns() {
        sql("create table my_table(i, s) as select 1 as a, 'test' as b");
        List<List<?>> res = sql("select i, s from my_table");

        assertEquals(1, res.size());

        List<?> row = res.get(0);

        assertEquals(1, row.get(0));
        assertEquals("test", row.get(1));

        assertThrows("select a from my_table", IgniteSQLException.class, "Column 'A' not found in any table");
    }

    /**
     * Creates table with options as select.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void createTableAsSelectWithOptions() {
        sql("create table my_table(s, i) with cache_name=\"CacheWithOpts\", cache_group=\"CacheGroup\" as select '1', 1");
        List<List<?>> res = sql("select * from my_table");

        assertEquals(1, res.size());

        List<?> row = res.get(0);

        assertEquals("1", row.get(0));
        assertEquals(1, row.get(1));

        IgniteCache<?, ?> cache = client.cache("CacheWithOpts");
        assertNotNull(cache);
        assertEquals("CacheGroup", cache.getConfiguration(CacheConfiguration.class).getGroupName());
    }

    /**
     * Creates table as select with dynamic parameters to query.
     */
    @Test
    public void createTableAsSelectWithParameters() {
        sql("create table my_table(s, i) as select cast(? as varchar), cast(? as int)", "a", 1);
        List<List<?>> res = sql("select * from my_table");

        assertEquals(1, res.size());

        List<?> row = res.get(0);

        assertEquals("a", row.get(0));
        assertEquals(1, row.get(1));
    }

    /** */
    @Test
    public void createTableAsSelectWrongColumnsCount() {
        GridTestUtils.assertThrowsAnyCause(log,
            () -> sql("create table my_table(i, s1, s2) as select 1, 'test'"),
            IgniteSQLException.class, "Number of columns");
    }

    /**
     * Creates table as select from another table.
     */
    @Test
    public void createTableAsSelectFromDistributedTable() {
        sql("create table my_table1(i) as select x from table(system_range(1, 100))");

        assertEquals(100L, sql("select count(*) from my_table1").get(0).get(0));

        sql("create table my_table2(i) as select * from my_table1");

        assertEquals(100L, sql("select count(*) from my_table2").get(0).get(0));
    }

    /**
     * Drops a table created in a default schema.
     */
    @Test
    public void dropTableDefaultSchema() {
        Set<String> cachesBefore = new HashSet<>(client.cacheNames());

        sql("create table my_table (id int primary key, val varchar)");

        Set<String> cachesAfter = new HashSet<>(client.cacheNames());
        cachesAfter.removeAll(cachesBefore);

        assertThat(cachesAfter, hasSize(1));

        String createdCacheName = cachesAfter.iterator().next();

        sql("drop table my_table");

        cachesAfter = new HashSet<>(client.cacheNames());
        cachesAfter.removeAll(cachesBefore);

        assertThat(cachesAfter, hasSize(0));

        assertThat(client.cachex(createdCacheName), nullValue());
    }

    /**
     * Drops a table created in a custom schema.
     */
    @Test
    public void dropTableCustomSchema() {
        Set<String> cachesBefore = new HashSet<>(client.cacheNames());

        sql("create table my_schema.my_table (id int primary key, val varchar)");

        Set<String> cachesAfter = new HashSet<>(client.cacheNames());
        cachesAfter.removeAll(cachesBefore);

        assertThat(cachesAfter, hasSize(1));

        String createdCacheName = cachesAfter.iterator().next();

        sql("drop table my_schema.my_table");

        cachesAfter = new HashSet<>(client.cacheNames());
        cachesAfter.removeAll(cachesBefore);

        assertThat(cachesAfter, hasSize(0));

        assertThat(client.cachex(createdCacheName), nullValue());
    }

    /**
     * Drops a table several times with and without
     * specifying IF EXISTS.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void dropTableIfExists() {
        sql("create table my_schema.my_table (id int primary key, val varchar)");

        GridTestUtils.assertThrows(log,
            () -> sql("drop table my_table"),
            IgniteSQLException.class, "Table doesn't exist: MY_TABLE");

        sql("drop table my_schema.my_table");

        GridTestUtils.assertThrows(log,
            () -> sql("drop table my_schema.my_table"),
            IgniteSQLException.class, "Table doesn't exist: MY_TABLE");

        sql("drop table if exists my_schema.my_table");
    }

    /**
     * Add/drop column simple case.
     */
    @Test
    public void alterTableAddDropSimpleCase() {
        sql("create table my_table (id int primary key, val varchar)");

        sql("alter table my_table add column val2 varchar");

        sql("insert into my_table (id, val, val2) values (0, '1', '2')");

        List<List<?>> res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals("2", res.get(0).get(2));

        sql("alter table my_table drop column val2");

        res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Add/drop two columns at the same time.
     */
    @Test
    public void alterTableAddDropTwoColumns() {
        sql("create table my_table (id int primary key, val varchar)");

        sql("alter table my_table add column (val2 varchar, val3 int)");

        sql("insert into my_table (id, val, val2, val3) values (0, '1', '2', 3)");

        List<List<?>> res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals("2", res.get(0).get(2));
        assertEquals(3, res.get(0).get(3));

        sql("alter table my_table drop column (val2, val3)");

        res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Add/drop column for table in custom schema.
     */
    @Test
    public void alterTableAddDropCustomSchema() {
        sql("create table my_schema.my_table (id int primary key, val varchar)");

        sql("alter table my_schema.my_table add column val2 varchar");

        sql("insert into my_schema.my_table (id, val, val2) values (0, '1', '2')");

        List<List<?>> res = sql("select * from my_schema.my_table ");

        assertEquals(1, res.size());
        assertEquals("2", res.get(0).get(2));

        sql("alter table my_schema.my_table drop column val2");

        res = sql("select * from my_schema.my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Add/drop column if table exists.
     */
    @Test
    public void alterTableAddDropIfTableExists() {
        assertThrows("alter table my_table add val2 varchar", IgniteSQLException.class, "Table doesn't exist");

        sql("alter table if exists my_table add column val2 varchar");

        assertThrows("alter table my_table drop column val2", IgniteSQLException.class, "Table doesn't exist");

        sql("alter table if exists my_table drop column val2");

        sql("create table my_table (id int primary key, val varchar)");

        sql("alter table if exists my_table add column val3 varchar");

        sql("insert into my_table (id, val, val3) values (0, '1', '2')");

        List<List<?>> res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals("2", res.get(0).get(2));

        sql("alter table if exists my_table drop column val3");

        assertThrows("alter table if exists my_table drop column val3", IgniteSQLException.class,
            "Column doesn't exist");

        res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Add/drop column if column not exists/exists.
     */
    @Test
    public void alterTableAddDropIfColumnExists() {
        sql("create table my_table (id int primary key, val varchar)");

        sql("insert into my_table (id, val) values (0, '1')");

        assertThrows("alter table my_table add column val varchar", IgniteSQLException.class,
            "Column already exist");

        sql("alter table my_table add column if not exists val varchar");

        List<List<?>> res = sql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());

        assertThrows("alter table my_table drop column val2", IgniteSQLException.class,
            "Column doesn't exist");

        sql("alter table my_table drop column if exists val2");

        res = sql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());

        sql("alter table my_table add column if not exists val3 varchar");

        res = sql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());

        sql("alter table my_table drop column if exists val3");

        res = sql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());

        // Mixing existsing and not existsing columns
        sql("alter table my_table add column if not exists (val varchar, val4 varchar)");

        res = sql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());

        sql("alter table my_table drop column if exists (val4, val5)");

        res = sql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Add not null column.
     */
    @Test
    public void alterTableAddNotNullColumn() {
        sql("create table my_table (id int primary key, val varchar)");

        sql("alter table my_table add column val2 varchar not null");

        assertThrows("insert into my_table (id, val, val2) values (0, '1', null)", IgniteSQLException.class,
            "Null value is not allowed");

        sql("insert into my_table (id, val, val2) values (0, '1', '2')");

        List<List<?>> res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals("2", res.get(0).get(2));
    }

    /**
     * Drop forbidden column.
     */
    @Test
    public void alterTableDropForbiddenColumn() {
        sql("create table my_table (id int primary key, val varchar, val2 varchar)");

        sql("create index my_index on my_table(val)");

        sql("insert into my_table (id, val, val2) values (0, '1', '2')");

        assertThrows("alter table my_table drop column id", IgniteSQLException.class,
            "Cannot drop column");

        assertThrows("alter table my_table drop column val", IgniteSQLException.class,
            "Cannot drop column");

        List<List<?>> res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());

        sql("alter table my_table drop column val2");

        res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Alter table from server and client nodes.
     */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-16292")
    public void alterTableServerAndClient() throws Exception {
        sql(grid(0), "create table my_table (id int primary key, val varchar)");

        sql(grid(0), "alter table my_table add column val2 varchar");

        sql(grid(0), "insert into my_table (id, val, val2) values (0, '1', '2')");

        List<List<?>> res = sql(grid(0), "select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());

        sql(grid(0), "drop table my_table");

        awaitPartitionMapExchange();

        sql("create table my_table (id int primary key, val varchar)");

        sql("alter table my_table add column val2 varchar");

        sql("insert into my_table (id, val, val2) values (0, '1', '2')");

        res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());

        awaitPartitionMapExchange();

        sql(grid(0), "alter table my_table drop column val2");

        awaitPartitionMapExchange();

        res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Drop and add the same column with different NOT NULL modificator.
     */
    @Test
    public void alterTableDropAddColumn() {
        sql("create table my_table (id int primary key, val varchar, val2 varchar)");

        sql("insert into my_table (id, val, val2) values (0, '1', '2')");

        sql("alter table my_table drop column val2");

        List<List<?>> res = sql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());

        sql("alter table my_table add column val2 varchar not null");

        res = sql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());
        // The command DROP COLUMN does not remove actual data from the cluster, it's a known and documented limitation.
        assertEquals("2", res.get(0).get(2));

        assertThrows("insert into my_table (id, val, val2) values (1, '2', null)", IgniteSQLException.class,
            "Null value is not allowed");

        sql("insert into my_table (id, val, val2) values (1, '2', '3')");

        assertEquals(2, sql("select * from my_table").size());
    }

    /**
     * Drop or add column and check indexes works.
     */
    @Test
    public void alterTableChangeColumnAndUseIndex() {
        sql("create table my_table(c1 int, c2 int, c3 int, c4 int)");
        sql("create index my_index on my_table(c4)");

        for (int i = 0; i < 100; i++)
            sql("insert into my_table values (1, 2, 3, ?)", i);

        assertQuery("select * from my_table where c4 = 50")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "MY_TABLE", "MY_INDEX"))
            .returns(1, 2, 3, 50)
            .check();

        sql("alter table my_table add column c5 int");

        assertQuery("select * from my_table where c4 = 50")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "MY_TABLE", "MY_INDEX"))
            .returns(1, 2, 3, 50, null)
            .check();

        sql("alter table my_table drop column c2");

        assertQuery("select * from my_table where c4 = 50")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "MY_TABLE", "MY_INDEX"))
            .returns(1, 3, 50, null)
            .check();

        sql("alter table my_table drop column (c1, c3)");

        assertQuery("select * from my_table where c4 = 50")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "MY_TABLE", "MY_INDEX"))
            .returns(50, null)
            .check();

        sql("alter table my_table drop column (c5)");

        assertQuery("select * from my_table where c4 = 50")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "MY_TABLE", "MY_INDEX"))
            .returns(50)
            .check();
    }

    /**
     * Alter table logging/nologing.
     */
    @Test
    public void alterTableLogging() {
        String cacheName = "cache";

        client.getOrCreateCache(new CacheConfiguration<>(cacheName)
            .setDataRegionName(PERSISTENT_DATA_REGION).setIndexedTypes(Integer.class, Integer.class));

        assertTrue(client.cluster().isWalEnabled(cacheName));

        sql("alter table \"" + cacheName + "\".Integer nologging");

        assertFalse(client.cluster().isWalEnabled(cacheName));

        sql("alter table \"" + cacheName + "\".Integer logging");

        assertTrue(client.cluster().isWalEnabled(cacheName));
    }

    /**
     * Tests that multiline statements with DDL and DML works as expected.
     */
    @Test
    public void testMulitlineWithCreateTable() {
        String multiLineQuery = "CREATE TABLE test (val0 int primary key, val1 varchar);" +
            "INSERT INTO test(val0, val1) VALUES (0, 'test0');" +
            "ALTER TABLE test ADD COLUMN val2 int;" +
            "INSERT INTO test(val0, val1, val2) VALUES(1, 'test1', 10);" +
            "ALTER TABLE test DROP COLUMN val2;";

        sql(multiLineQuery);

        List<List<?>> res = sql("SELECT * FROM test order by val0");
        assertEquals(2, res.size());

        for (int i = 0; i < res.size(); i++) {
            List<?> row = res.get(i);

            assertEquals(2, row.size());
            assertEquals(i, row.get(0));
            assertEquals("test" + i, row.get(1));
        }
    }

    /**
     * Matcher to verify that an object of the expected type and matches the given predicat.
     *
     * @param desc Description for this matcher.
     * @param pred Addition check that would be applied to the object.
     * @return {@code true} in case the object if instance of the given class and matches the predicat.
     */
    private static Matcher<CacheConfiguration<?, ?>> matches(String desc, Predicate<CacheConfiguration<?, ?>> pred) {
        return new CustomMatcher<CacheConfiguration<?, ?>>(desc) {
            @Override public boolean matches(Object item) {
                return item instanceof CacheConfiguration && pred.test((CacheConfiguration<?, ?>)item);
            }
        };
    }
}
