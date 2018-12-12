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

import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

public class IgniteCacheSqlConfigurationSelfTest extends GridCommonAbstractTest {

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    public void testSimpleConfiguration() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();

        IgniteCache<Long, Long> cache = jcache(grid(0), defaultCacheConfiguration().setName("someCache")
            .setIndexedTypes(Long.class, Long.class), "someCache");


        cache.put(1L,2L );
    }


    public void testSimpleDDL() {
        IgniteCache<Object, Object> cache = jcache(grid(0), defaultCacheConfiguration().setName("cacheDDL"), "cacheDDL");

        cache.query(new SqlFieldsQuery("CREATE TABLE PUBLIC.TEST_SIMPLE (id DECIMAL(4,2) PRIMARY KEY, val BIGINT NOT NULL)")).getAll();
        cache.query(new SqlFieldsQuery("INSERT INTO  PUBLIC.TEST_SIMPLE VALUES (100001, 2)"));
    }

    /**
     * @param qryEntity Query entity.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(QueryEntity qryEntity) {
        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setQueryEntities(Collections.singletonList(qryEntity));

        return cache;
    }
}
