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

package org.apache.ignite.internal.processors.cache.query;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;

/**
 * {@link ScanQueryConcurrentUpdatesAbstractTest} with caches created, updates and destroyed using SQL DDL queries.
 */
public class ScanQueryConcurrentSqlUpdatesTest extends ScanQueryConcurrentUpdatesAbstractTest {
    /**
     * A name for a cache that will be used to execute DDL queries.
     */
    private static final String DUMMY_CACHE_NAME = "dummy";

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Integer> createCache(String cacheName, CacheMode cacheMode,
                                                                  Duration expiration) {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setCacheMode(cacheMode);
        if (expiration != null) {
            cacheCfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(expiration));
            cacheCfg.setEagerTtl(true);
        }

        IgniteEx ignite = grid(0);
        ignite.addCacheConfiguration(cacheCfg);

        ignite.getOrCreateCache(DUMMY_CACHE_NAME).query(new SqlFieldsQuery("CREATE TABLE " + cacheName + " " +
            "(key int primary key, val int) " +
            "WITH \"template=" + cacheName + ",wrap_value=false\""));

        return ignite.cache("SQL_PUBLIC_" + cacheName.toUpperCase());
    }

    /** {@inheritDoc} */
    @Override protected void updateCache(IgniteCache<Integer, Integer> cache, int recordsNum) {
        String tblName = tableName(cache);

        for (int i = 0; i < recordsNum; i++) {
            cache.query(new SqlFieldsQuery(
                "INSERT INTO " + tblName + " (key, val) " +
                "VALUES (" + i + ", " + i + ")"));
        }
    }

    /** {@inheritDoc} */
    @Override protected void destroyCache(IgniteCache<Integer, Integer> cache) {
        grid(0).cache(DUMMY_CACHE_NAME).query(new SqlFieldsQuery("DROP TABLE " + tableName(cache)));
    }

    /**
     * @param cache Cache to determine a table name for.
     * @return Name of the table corresponding to the provided cache.
     */
    @SuppressWarnings("unchecked")
    private String tableName(IgniteCache<Integer, Integer> cache) {
        CacheConfiguration<Integer, Integer> cacheCfg =
                (CacheConfiguration<Integer, Integer>) cache.getConfiguration(CacheConfiguration.class);
        QueryEntity qe = cacheCfg.getQueryEntities().iterator().next();

        return qe.getTableName();
    }
}
