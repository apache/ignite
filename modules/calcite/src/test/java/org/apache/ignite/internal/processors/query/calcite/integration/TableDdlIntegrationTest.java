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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.util.IgniteUtils.map;
import static org.apache.ignite.testframework.GridTestUtils.hasSize;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/** */
public class TableDdlIntegrationTest extends GridCommonAbstractTest {
    /** */
    private static final String CLIENT_NODE_NAME = "client";

    /** */
    private static final String DATA_REGION_NAME = "test_data_region";

    /** */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);

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
            IgniteSQLException.class, "Table doesn't exist: MY_TABLE]");

        executeSql("drop table my_schema.my_table");

        GridTestUtils.assertThrows(log,
            () -> executeSql("drop table my_schema.my_table"),
            IgniteSQLException.class, "Table doesn't exist: MY_TABLE]");

        executeSql("drop table if exists my_schema.my_table");
    }

    /** */
    private List<List<?>> executeSql(String sql) {
        List<FieldsQueryCursor<List<?>>> cur = queryProcessor().query(null, "PUBLIC", sql);

        try (QueryCursor<List<?>> srvCursor = cur.get(0)) {
            return srvCursor.getAll();
        }
    }

    /** */
    private CalciteQueryProcessor queryProcessor() {
        return Commons.lookupComponent(client.context(), CalciteQueryProcessor.class);
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
