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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Check's correct node behavior on join in case sql table was changed.
 */
public class SqlCacheConfigurationCheckOnNodeJoinTest extends GridCommonAbstractTest {
    /** Schema name. */
    private static final String SCHEMA_NAME = "PUBLIC";

    /** Table name. */
    private static final String TABLE_NAME = "PERSON";

    /** Create table sql. */
    private static final String CREATE_TABLE_SQL = "CREATE TABLE " + TABLE_NAME +
        " (id int primary key, name varchar) WITH \"backups=1\"";

    /** Select sql. */
    private static final String SELECT_SQL = "SELECT id from " + TABLE_NAME + " where name = 'foo'";

    /** Second select sql. */
    private static final String SELECT_SQL_2 = "SELECT id2 from " + TABLE_NAME + " where name = 'foo'";

    /** Insert sql. */
    private static final String INSERT_SQL = "INSERT INTO " + TABLE_NAME + "(id, name) VALUES(?,?)";

    /** Drop table sql. */
    private static final String DROP_TABLE_SQL = "DROP TABLE " + TABLE_NAME;

    /** Alter table sql. */
    private static final String ALTER_TABLE_SQL = "ALTER TABLE " + TABLE_NAME + " ADD COLUMN (id2 int)";

    /** Update table sql. */
    private static final String UPDATE_TABLE_SQL = "UPDATE " + TABLE_NAME + " SET id2 = ? WHERE id = ?";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration drCfg = new DataRegionConfiguration().setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(drCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    public void testCreateTableAfterNodeShutdown() throws Exception {
        testCreateTableAfterNodeShutdown(false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testCreateTableAfterNodeShutdownAndClusterRestart() throws Exception {
        testCreateTableAfterNodeShutdown(true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testDropTableAfterNodeShutdown() throws Exception {
        testDropTableAfterNodeShutdown(false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testDropTableAfterNodeShutdownAndClusterRestart() throws Exception {
        testDropTableAfterNodeShutdown(true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testAlterTableAfterNodeShutdown() throws Exception {
        testAlterTableAfterNodeShutdown(false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testAlterTableAfterNodeShutdownAndClusterRestart() throws Exception {
        testAlterTableAfterNodeShutdown(true);
    }

    /** */
    private void testAlterTableAfterNodeShutdown(boolean clusterRestart) throws Exception {
        startGrids(2);

        grid(0).cluster().active(true);

        awaitPartitionMapExchange();

        CacheConfiguration<Integer, Person> cacheCfg =
            new CacheConfiguration<Integer, Person>(DEFAULT_CACHE_NAME)
                .setBackups(1).setIndexedTypes(Integer.class, Person.class);

        IgniteCache<Integer, Person> cache0 = grid(0).getOrCreateCache(cacheCfg);

        cache0.query(new SqlFieldsQuery(CREATE_TABLE_SQL).setSchema(SCHEMA_NAME)).getAll();

        populateData(cache0);

        checkDataPresent(cache0, SELECT_SQL);
        checkDataPresent(grid(1).cache(DEFAULT_CACHE_NAME), SELECT_SQL);

        stopGrid(1);

        cache0.query(new SqlFieldsQuery(ALTER_TABLE_SQL).setSchema(SCHEMA_NAME)).getAll();

        updateData(cache0);

        checkDataPresent(cache0, SELECT_SQL_2);

        Ignite ignite1;

        if (clusterRestart) {
            stopAllGrids();

            startGrids(2);

            ignite1 = grid(1);

            ignite1.cluster().active(true);
        }
        else
            ignite1 = startGrid(1);

        IgniteCache<Integer, Person> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        checkDataPresent(cache1, SELECT_SQL_2);
    }

    /** */
    private void testCreateTableAfterNodeShutdown(boolean clusterRestart) throws Exception {
        startGrids(2);

        grid(0).cluster().active(true);

        awaitPartitionMapExchange();

        CacheConfiguration<Integer, Person> cacheCfg =
            new CacheConfiguration<Integer, Person>(DEFAULT_CACHE_NAME)
                .setBackups(1).setIndexedTypes(Integer.class, Person.class);

        IgniteCache<Integer, Person> cache0 = grid(0).getOrCreateCache(cacheCfg);

        stopGrid(1);

        cache0.query(new SqlFieldsQuery(CREATE_TABLE_SQL).setSchema(SCHEMA_NAME)).getAll();

        populateData(cache0);

        checkDataPresent(cache0, SELECT_SQL);

        Ignite ignite1;

        if (clusterRestart) {
            stopAllGrids();

            startGrids(2);

            ignite1 = grid(1);

            ignite1.cluster().active(true);
        }
        else
            ignite1 = startGrid(1);

        IgniteCache<Integer, Person> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        checkDataPresent(cache1, SELECT_SQL);
    }

    /** */
    private void testDropTableAfterNodeShutdown(boolean clusterRestart) throws Exception {
        startGrids(2);

        grid(0).cluster().active(true);

        awaitPartitionMapExchange();

        CacheConfiguration<Integer, Person> cacheCfg =
            new CacheConfiguration<Integer, Person>(DEFAULT_CACHE_NAME)
                .setBackups(1).setIndexedTypes(Integer.class, Person.class);

        IgniteCache<Integer, Person> cache0 = grid(0).getOrCreateCache(cacheCfg);

        cache0.query(new SqlFieldsQuery(CREATE_TABLE_SQL).setSchema(SCHEMA_NAME)).getAll();

        populateData(cache0);

        checkDataPresent(cache0, SELECT_SQL);

        stopGrid(1);

        cache0.query(new SqlFieldsQuery(DROP_TABLE_SQL).setSchema(SCHEMA_NAME)).getAll();

        Ignite ignite1;

        if (clusterRestart) {
            stopAllGrids();

            startGrids(2);

            ignite1 = grid(1);

            ignite1.cluster().active(true);
        }
        else
            ignite1 = startGrid(1);

        ignite1.cache(DEFAULT_CACHE_NAME);
    }

    /** */
    private void populateData(IgniteCache<Integer, Person> cache) {
        for (int i = 0; i < 30; i++) {
            cache.query(
                new SqlFieldsQuery(INSERT_SQL)
                    .setSchema(SCHEMA_NAME)
                    .setArgs(i, i % 2 == 0 ? "foo" : "bar")
            ).getAll();
        }
    }

    /** */
    private void updateData(IgniteCache<Integer, Person> cache) {
        List<List<?>> ids = cache.query(new SqlFieldsQuery(SELECT_SQL).setSchema(SCHEMA_NAME)).getAll();

        assertFalse(ids.isEmpty());

        for (List<?> l : ids) {
            assertEquals(1, l.size());

            cache.query(
                new SqlFieldsQuery(UPDATE_TABLE_SQL)
                    .setSchema(SCHEMA_NAME)
                    .setArgs(l.get(0), l.get(0))
            ).getAll();
        }
    }

    /** */
    private void checkDataPresent(IgniteCache<Integer, Person> cache, String selectSql) {
        List<List<?>> ids = cache.query(new SqlFieldsQuery(selectSql).setSchema(SCHEMA_NAME)).getAll();

        assertFalse(ids.isEmpty());

        for (List<?> l : ids) {
            assertEquals(1, l.size());

            assertEquals(0, ((Integer)l.get(0)) % 2);
        }
    }
}