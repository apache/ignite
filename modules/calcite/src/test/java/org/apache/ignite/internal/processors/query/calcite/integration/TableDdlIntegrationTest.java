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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.map;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.hasSize;
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

        executeSql("create table my_table (id int primary key, val varchar)");

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
        executeSql("create table my_table (" +
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

        executeSql("insert into my_table values (" +
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

        List<List<?>> res = executeSql("select * from my_table");

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

        executeSql("create table my_table (id1 int, id2 int, val varchar, primary key(id1, id2)) with " +
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
     * Creates several tables with specified templates,
     * and verifies created caches.
     */
    @Test
    @SuppressWarnings("rawtypes")
    public void createTableWithTemplate() {
        Set<String> cachesBefore = new HashSet<>(client.cacheNames());

        executeSql("create table my_table_replicated (id int, val varchar) with template=replicated, cache_name=repl");
        executeSql("create table my_table_partitioned (id int, val varchar) with template=partitioned, cache_name=part");

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
        executeSql("create table my_table (id int, val varchar)");

        GridTestUtils.assertThrows(log,
            () -> executeSql("create table my_table (id int, val varchar)"),
            IgniteSQLException.class, "Table already exists: MY_TABLE");

        executeSql("create table if not exists my_table (id int, val varchar)");
    }

    /**
     * Creates a table without a primary key and then insert a few rows.
     */
    @Test
    public void createTableWithoutPk() {
        executeSql("create table my_table (f1 int, f2 varchar)");

        executeSql("insert into my_table(f1, f2) values (1, '1'),(2, '2')");
        executeSql("insert into my_table values (1, '1'),(2, '2')");

        assertThat(executeSql("select * from my_table"), hasSize(4));
    }

    /**
     * Creates a table in a custom schema.
     */
    @Test
    public void createTableCustomSchema() {
        executeSql("create table my_schema.my_table (f1 int, f2 varchar)");

        executeSql("insert into my_schema.my_table(f1, f2) values (1, '1'),(2, '2')");
        executeSql("insert into my_schema.my_table values (1, '1'),(2, '2')");

        assertThat(executeSql("select * from my_schema.my_table"), hasSize(4));
    }

    /**
     * Creates a table for already existing cache.
     */
    @Test
    public void createTableOnExistingCache() {
        IgniteCache<Object, Object> cache = client.getOrCreateCache("my_cache");

        executeSql("create table my_schema.my_table (f1 int, f2 varchar) with cache_name=\"my_cache\"");

        executeSql("insert into my_schema.my_table(f1, f2) values (1, '1'),(2, '2')");

        assertThat(executeSql("select * from my_schema.my_table"), hasSize(2));

        assertEquals(2, cache.size(CachePeekMode.PRIMARY));
    }

    /**
     * Drops a table created in a default schema.
     */
    @Test
    public void dropTableDefaultSchema() {
        Set<String> cachesBefore = new HashSet<>(client.cacheNames());

        executeSql("create table my_table (id int primary key, val varchar)");

        Set<String> cachesAfter = new HashSet<>(client.cacheNames());
        cachesAfter.removeAll(cachesBefore);

        assertThat(cachesAfter, hasSize(1));

        String createdCacheName = cachesAfter.iterator().next();

        executeSql("drop table my_table");

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

        executeSql("create table my_schema.my_table (id int primary key, val varchar)");

        Set<String> cachesAfter = new HashSet<>(client.cacheNames());
        cachesAfter.removeAll(cachesBefore);

        assertThat(cachesAfter, hasSize(1));

        String createdCacheName = cachesAfter.iterator().next();

        executeSql("drop table my_schema.my_table");

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
        executeSql("create table my_schema.my_table (id int primary key, val varchar)");

        GridTestUtils.assertThrows(log,
            () -> executeSql("drop table my_table"),
            IgniteSQLException.class, "Table doesn't exist: MY_TABLE");

        executeSql("drop table my_schema.my_table");

        GridTestUtils.assertThrows(log,
            () -> executeSql("drop table my_schema.my_table"),
            IgniteSQLException.class, "Table doesn't exist: MY_TABLE");

        executeSql("drop table if exists my_schema.my_table");
    }

    /**
     * Add/drop column simple case.
     */
    @Test
    public void alterTableAddDropSimpleCase() {
        executeSql("create table my_table (id int primary key, val varchar)");

        executeSql("alter table my_table add column val2 varchar");

        executeSql("insert into my_table (id, val, val2) values (0, '1', '2')");

        List<List<?>> res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals("2", res.get(0).get(2));

        executeSql("alter table my_table drop column val2");

        res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Add/drop two columns at the same time.
     */
    @Test
    public void alterTableAddDropTwoColumns() {
        executeSql("create table my_table (id int primary key, val varchar)");

        executeSql("alter table my_table add column (val2 varchar, val3 int)");

        executeSql("insert into my_table (id, val, val2, val3) values (0, '1', '2', 3)");

        List<List<?>> res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals("2", res.get(0).get(2));
        assertEquals(3, res.get(0).get(3));

        executeSql("alter table my_table drop column (val2, val3)");

        res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Add/drop column for table in custom schema.
     */
    @Test
    public void alterTableAddDropCustomSchema() {
        executeSql("create table my_schema.my_table (id int primary key, val varchar)");

        executeSql("alter table my_schema.my_table add column val2 varchar");

        executeSql("insert into my_schema.my_table (id, val, val2) values (0, '1', '2')");

        List<List<?>> res = executeSql("select * from my_schema.my_table ");

        assertEquals(1, res.size());
        assertEquals("2", res.get(0).get(2));

        executeSql("alter table my_schema.my_table drop column val2");

        res = executeSql("select * from my_schema.my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Add/drop column if table exists.
     */
    @Test
    public void alterTableAddDropIfTableExists() {
        assertThrows("alter table my_table add val2 varchar", IgniteSQLException.class, "Table doesn't exist");

        executeSql("alter table if exists my_table add column val2 varchar");

        assertThrows("alter table my_table drop column val2", IgniteSQLException.class, "Table doesn't exist");

        executeSql("alter table if exists my_table drop column val2");

        executeSql("create table my_table (id int primary key, val varchar)");

        executeSql("alter table if exists my_table add column val3 varchar");

        executeSql("insert into my_table (id, val, val3) values (0, '1', '2')");

        List<List<?>> res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals("2", res.get(0).get(2));

        executeSql("alter table if exists my_table drop column val3");

        assertThrows("alter table if exists my_table drop column val3", IgniteSQLException.class,
            "Column doesn't exist");

        res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Add/drop column if column not exists/exists.
     */
    @Test
    public void alterTableAddDropIfColumnExists() {
        executeSql("create table my_table (id int primary key, val varchar)");

        executeSql("insert into my_table (id, val) values (0, '1')");

        assertThrows("alter table my_table add column val varchar", IgniteSQLException.class,
            "Column already exist");

        executeSql("alter table my_table add column if not exists val varchar");

        List<List<?>> res = executeSql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());

        assertThrows("alter table my_table drop column val2", IgniteSQLException.class,
            "Column doesn't exist");

        executeSql("alter table my_table drop column if exists val2");

        res = executeSql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());

        executeSql("alter table my_table add column if not exists val3 varchar");

        res = executeSql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());

        executeSql("alter table my_table drop column if exists val3");

        res = executeSql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());

        // Mixing existsing and not existsing columns
        executeSql("alter table my_table add column if not exists (val varchar, val4 varchar)");

        res = executeSql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());

        executeSql("alter table my_table drop column if exists (val4, val5)");

        res = executeSql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Add not null column.
     */
    @Test
    public void alterTableAddNotNullColumn() {
        executeSql("create table my_table (id int primary key, val varchar)");

        executeSql("alter table my_table add column val2 varchar not null");

        assertThrows("insert into my_table (id, val, val2) values (0, '1', null)", IgniteSQLException.class,
            "Null value is not allowed");

        executeSql("insert into my_table (id, val, val2) values (0, '1', '2')");

        List<List<?>> res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals("2", res.get(0).get(2));
    }

    /**
     * Drop forbidden column.
     */
    @Test
    public void alterTableDropForbiddenColumn() {
        executeSql("create table my_table (id int primary key, val varchar, val2 varchar)");

        executeSql("create index my_index on my_table(val)");

        executeSql("insert into my_table (id, val, val2) values (0, '1', '2')");

        assertThrows("alter table my_table drop column id", IgniteSQLException.class,
            "Cannot drop column");

        assertThrows("alter table my_table drop column val", IgniteSQLException.class,
            "Cannot drop column");

        List<List<?>> res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());

        executeSql("alter table my_table drop column val2");

        res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Alter table from server and client nodes.
     */
    @Test
    public void alterTableServerAndClient() throws Exception {
        executeSql(grid(0), "create table my_table (id int primary key, val varchar)");

        executeSql(grid(0), "alter table my_table add column val2 varchar");

        executeSql(grid(0), "insert into my_table (id, val, val2) values (0, '1', '2')");

        List<List<?>> res = executeSql(grid(0), "select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());

        executeSql(grid(0), "drop table my_table");

        awaitPartitionMapExchange();

        executeSql("create table my_table (id int primary key, val varchar)");

        executeSql("alter table my_table add column val2 varchar");

        executeSql("insert into my_table (id, val, val2) values (0, '1', '2')");

        res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());

        awaitPartitionMapExchange();

        executeSql(grid(0), "alter table my_table drop column val2");

        awaitPartitionMapExchange();

        res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());
    }

    /**
     * Drop and add the same column with different NOT NULL modificator.
     */
    @Test
    public void alterTableDropAddColumn() {
        executeSql("create table my_table (id int primary key, val varchar, val2 varchar)");

        executeSql("insert into my_table (id, val, val2) values (0, '1', '2')");

        executeSql("alter table my_table drop column val2");

        List<List<?>> res = executeSql("select * from my_table ");

        assertEquals(1, res.size());
        assertEquals(2, res.get(0).size());

        executeSql("alter table my_table add column val2 varchar not null");

        res = executeSql("select * from my_table ");
        assertEquals(1, res.size());
        assertEquals(3, res.get(0).size());
        // The command DROP COLUMN does not remove actual data from the cluster, it's a known and documented limitation.
        assertEquals("2", res.get(0).get(2));

        assertThrows("insert into my_table (id, val, val2) values (1, '2', null)", IgniteSQLException.class,
            "Null value is not allowed");

        executeSql("insert into my_table (id, val, val2) values (1, '2', '3')");

        assertEquals(2, executeSql("select * from my_table").size());
    }

    /**
     * Alter table logging/nologing.
     */
    @Test
    public void alterTableLogging() {
        String cacheName = "cache";

        IgniteCache<Object, Object> cache = client.getOrCreateCache(new CacheConfiguration<>(cacheName)
            .setDataRegionName(PERSISTENT_DATA_REGION).setIndexedTypes(Integer.class, Integer.class));

        assertTrue(client.cluster().isWalEnabled(cacheName));

        executeSql("alter table \"" + cacheName + "\".Integer nologging");

        assertFalse(client.cluster().isWalEnabled(cacheName));

        executeSql("alter table \"" + cacheName + "\".Integer logging");

        assertTrue(client.cluster().isWalEnabled(cacheName));
    }

    /**
     * Asserts that executeSql throws an exception.
     *
     * @param sql Query.
     * @param cls Exception class.
     * @param msg Error message.
     */
    private void assertThrows(String sql, Class<? extends Exception> cls, String msg) {
        assertThrowsAnyCause(log, () -> executeSql(sql), cls, msg);
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
