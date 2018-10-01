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

public class SqlCacheConfigurationCheckOnNodeJoinTest extends GridCommonAbstractTest {
    private static final String TABLE_NAME = "PERSON";

    private static final String CREATE_TABLE_SQL = "CREATE TABLE " + TABLE_NAME + " (id int primary key, name varchar) WITH \"backups=1\"";

    private static final String SELECT_SQL = "SELECT id from " + TABLE_NAME + " where name = 'foo'";

    private static final String INSERT_SQL = "INSERT INTO " + TABLE_NAME + "(id, name) VALUES(?,?)";

    private static final String DROP_TABLE_SQL = "DROP TABLE " + TABLE_NAME;

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

    public void test() throws Exception {
        testCreateTableAfterNodeShutdown(false);
    }

    public void test2() throws Exception {
        testDropTableAfterNodeShutdown(false);
    }

    private void testCreateTableAfterNodeShutdown(boolean clusterRestart) throws Exception {
        startGrids(2);

        grid(0).cluster().active(true);

        awaitPartitionMapExchange();

        CacheConfiguration<Integer, Person> cacheCfg =
            new CacheConfiguration<Integer, Person>(DEFAULT_CACHE_NAME)
                .setBackups(1).setIndexedTypes(Integer.class, Person.class);

        IgniteCache<Integer, Person> cache0 = grid(0).getOrCreateCache(cacheCfg);

        stopGrid(1);

        cache0.query(new SqlFieldsQuery(CREATE_TABLE_SQL).setSchema("PUBLIC")).getAll();

        for (int i = 0; i < 30; i++)
            cache0.query(new SqlFieldsQuery(INSERT_SQL).setSchema("PUBLIC").setArgs(i, i % 2 == 0 ? "foo" : "bar")).getAll();

        List<List<?>> ids = cache0.query(new SqlFieldsQuery(SELECT_SQL).setSchema("PUBLIC")).getAll();

        assertFalse(ids.isEmpty());

        for (List<?> l : ids) {
            assertEquals(1, l.size());

            assertEquals(0, ((Integer)l.get(0)) % 2);
        }

        Ignite ignite1 = startGrid(1);

        IgniteCache<Integer, Person> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        ids = cache1.query(new SqlFieldsQuery(SELECT_SQL).setSchema("PUBLIC")).getAll();

        assertFalse(ids.isEmpty());

        for (List<?> l : ids) {
            assertEquals(1, l.size());

            assertEquals(0, ((Integer)l.get(0)) % 2);
        }
    }

    private void testDropTableAfterNodeShutdown(boolean clusterRestart) throws Exception {
        startGrids(2);

        grid(0).cluster().active(true);

        awaitPartitionMapExchange();

        CacheConfiguration<Integer, Person> cacheCfg =
            new CacheConfiguration<Integer, Person>(DEFAULT_CACHE_NAME)
                .setBackups(1).setIndexedTypes(Integer.class, Person.class);

        IgniteCache<Integer, Person> cache0 = grid(0).getOrCreateCache(cacheCfg);

        cache0.query(new SqlFieldsQuery(CREATE_TABLE_SQL).setSchema("PUBLIC")).getAll();

        for (int i = 0; i < 30; i++)
            cache0.query(new SqlFieldsQuery(INSERT_SQL).setSchema("PUBLIC").setArgs(i, i % 2 == 0 ? "foo" : "bar")).getAll();

        List<List<?>> ids = cache0.query(new SqlFieldsQuery(SELECT_SQL).setSchema("PUBLIC")).getAll();

        assertFalse(ids.isEmpty());

        for (List<?> l : ids) {
            assertEquals(1, l.size());

            assertEquals(0, ((Integer)l.get(0)) % 2);
        }

        stopGrid(1);

        cache0.query(new SqlFieldsQuery(DROP_TABLE_SQL).setSchema("PUBLIC")).getAll();

        Ignite ignite1 = startGrid(1);

        IgniteCache<Integer, Person> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);
    }
}