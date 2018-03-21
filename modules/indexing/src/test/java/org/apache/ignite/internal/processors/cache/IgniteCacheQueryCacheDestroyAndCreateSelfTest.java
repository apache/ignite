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

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Check NPE at the DmlStatementsProcessor on DML queries after cache destroy and re-create.
 */
public class IgniteCacheQueryCacheDestroyAndCreateSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int GRID_CNT = 2;

    /**
     * @throws Exception If failed.
     */
    public void testSqlSelectWithCacheRecreate() throws Exception {
        executeSqlWithCacheRecreate("select * from Integer");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlInsertWithCacheRecreate() throws Exception {
        executeSqlWithCacheRecreate("insert into Integer(_key, _val) values " +
            "('p1', 1)");
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlInsertWithCacheRecreateOnNewNode() throws Exception {
        executeSqlWithCacheRecreateOnNewNode("insert into Integer(_key, _val) values " +
            "('p1', 1)");
    }

    /**
     * @param sql Sql statement.
     * @throws Exception If failed.
     */
    private void executeSqlWithCacheRecreate(String sql) throws Exception {
        try {
            startGridsMultiThreaded(GRID_CNT);

            ignite(0).getOrCreateCache(personCacheConfig());

            grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql));

            grid(0).destroyCache(DEFAULT_CACHE_NAME);

            grid(0).getOrCreateCache(personCacheConfig());

            grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql));
        } finally {
            stopAllGrids();
        }
    }

    /**
     * @param sql Sql statement.
     * @throws Exception If failed.
     */
    private void executeSqlWithCacheRecreateOnNewNode(String sql) throws Exception {
        try {
            startGridsMultiThreaded(GRID_CNT);

            ignite(0).getOrCreateCache(personCacheConfig());

            grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql));

            grid(0).destroyCache(DEFAULT_CACHE_NAME);

            grid(0).getOrCreateCache(personCacheConfig());

            startGrid(GRID_CNT);

            grid(GRID_CNT).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql));
        } finally {
            stopAllGrids();
        }
    }

    /**
     * @return Cache configuration for non binary marshaller tests.
     */
    private CacheConfiguration personCacheConfig() {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(
            String.class, Integer.class
        );

        return cache;
    }
}
