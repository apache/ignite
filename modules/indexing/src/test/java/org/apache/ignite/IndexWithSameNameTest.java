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

package org.apache.ignite;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IndexWithSameNameTest extends GridCommonAbstractTest {
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
            )
        );

        return cfg;
    }

    @Test
    public void test() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.createCache(new CacheConfiguration<>("foo")
            .setGroupName("fooGroup")
        );

        IgniteCache<?, ?> cache = ignite.createCache(new CacheConfiguration<>("gateway_cache")
            .setSqlSchema("PUBLIC")
        );

        cache.query(new SqlFieldsQuery("CREATE TABLE T (i INT PRIMARY KEY, y INT)")).getAll();
        cache.query(new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS IDX ON T (i)")).getAll();

        cache.query(new SqlFieldsQuery("CREATE TABLE T2 (i INT PRIMARY KEY, y INT)")).getAll();
        cache.query(new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS IDX ON T2 (i)")).getAll();

        ignite.cluster().active(false);

        stopGrid(0);
        startGrid(0);
    }
}
