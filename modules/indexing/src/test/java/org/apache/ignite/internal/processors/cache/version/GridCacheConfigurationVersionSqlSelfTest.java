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
package org.apache.ignite.internal.processors.cache.version;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;

import static org.apache.ignite.internal.processors.cache.version.GridCacheConfigurationChangeAction.DESTROY;
import static org.apache.ignite.internal.processors.cache.version.GridCacheConfigurationChangeAction.META_CHANGED;
import static org.apache.ignite.internal.processors.cache.version.GridCacheConfigurationChangeAction.START;

/**
 *
 */
public class GridCacheConfigurationVersionSqlSelfTest extends GridCacheConfigurationVersionAbstractSelfTest {
    /** Schema name. */
    private static final String SCHEMA_NAME = "PUBLIC";

    /** Table name. */
    private static final String TABLE_NAME = "PERSON";

    /** Sql cache name. */
    private static final String SQL_CACHE_NAME = "SQL_" + SCHEMA_NAME + "_" + TABLE_NAME;

    /** Create table sql. */
    private static final String CREATE_TABLE_SQL = "CREATE TABLE " + TABLE_NAME + " (id int primary key, name varchar) WITH \"backups=1\"";

    /** Drop table sql. */
    private static final String DROP_TABLE_SQL = "DROP TABLE " + TABLE_NAME;

    /** Alter table sql. */
    private static final String ALTER_TABLE_SQL = "ALTER TABLE " + TABLE_NAME + " ADD COLUMN (id2 int)";

    /** */
    public void testRestartNode() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        performActionOnStartTestAfterClusterActivate(ignite);

        IgniteCache<Integer, Person> cache = ignite.cache(DEFAULT_CACHE_NAME);

        assertNull(ignite.context().cache().cacheDescriptor(SQL_CACHE_NAME));

        cache.query(new SqlFieldsQuery(CREATE_TABLE_SQL).setSchema(SCHEMA_NAME)).getAll();

        checkCacheVersion(ignite, SQL_CACHE_NAME, 1, START);

        stopAllGrids();

        ignite = startGrid(0);

        ignite.cluster().active(true);

        checkCacheVersion(ignite, SQL_CACHE_NAME, 1, START);
    }

    /** {@inheritDoc} */
    @Override protected int performActionsOnCache(
        int firstNodeId,
        int lastNodeId,
        int ver,
        IgniteEx ignite
    ) throws Exception {
        IgniteCache<Integer, Person> cache = ignite.cache(DEFAULT_CACHE_NAME);

        assertNull(ignite.context().cache().cacheDescriptor(SQL_CACHE_NAME));

        cache.query(new SqlFieldsQuery(CREATE_TABLE_SQL).setSchema(SCHEMA_NAME)).getAll();

        awaitCacheVersion(firstNodeId, lastNodeId, SQL_CACHE_NAME, ++ver, 5000L);

        for (int i = firstNodeId; i < lastNodeId; i++)
            checkCacheVersion(grid(i), SQL_CACHE_NAME, ver, START);

        cache.query(new SqlFieldsQuery(ALTER_TABLE_SQL).setSchema(SCHEMA_NAME)).getAll();

        awaitCacheVersion(firstNodeId, lastNodeId, SQL_CACHE_NAME, ++ver, 5000L);

        for (int i = firstNodeId; i < lastNodeId; i++)
            checkCacheVersion(grid(i), SQL_CACHE_NAME, ver, META_CHANGED);

        cache.query(new SqlFieldsQuery(DROP_TABLE_SQL).setSchema(SCHEMA_NAME)).getAll();

        awaitCacheVersion(firstNodeId, lastNodeId, SQL_CACHE_NAME, ++ver, 5000L);

        for (int i = firstNodeId; i < lastNodeId; i++)
            checkCacheVersion(grid(i), SQL_CACHE_NAME, ver, DESTROY);

        return ver;
    }

    /** {@inheritDoc} */
    @Override protected void performActionOnStartTestAfterClusterActivate(IgniteEx ignite) throws Exception {
        super.performActionOnStartTestAfterClusterActivate(ignite);

        assertNull(ignite.context().cache().cacheDescriptor(DEFAULT_CACHE_NAME));

        CacheConfiguration<Integer, Person> cacheCfg =
            new CacheConfiguration<Integer, Person>(DEFAULT_CACHE_NAME)
                .setBackups(1).setIndexedTypes(Integer.class, Person.class);

        ignite.getOrCreateCache(cacheCfg);

        checkCacheVersion(ignite, DEFAULT_CACHE_NAME, 1, START);
    }
}