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

import java.sql.Date;
import java.util.Collections;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.jupiter.api.Test;

/** */
public class QueryEntityValueColumnAliasTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final String KEY_COLUMN_NAME = "id";

    /** */
    private static final String VALUE_COLUMN_NAME = "value";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        SqlConfiguration sqlCfg = new SqlConfiguration()
            .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        cfg.setSqlConfiguration(sqlCfg);

        return cfg;
    }

    /** */
    @Test
    public void queryEntityValueColumnAliasTest() throws Exception {
        try (IgniteEx src = startGrid(0)) {
            QueryEntity qryEntity = new QueryEntity()
                .setTableName(CACHE_NAME)
                .setKeyFieldName(KEY_COLUMN_NAME)
                .setValueFieldName(VALUE_COLUMN_NAME)
                .addQueryField(KEY_COLUMN_NAME, Integer.class.getName(), null)
                .addQueryField(VALUE_COLUMN_NAME, Date.class.getName(), null);

            CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<Integer, Object>(qryEntity.getTableName())
                .setQueryEntities(Collections.singletonList(qryEntity));

            try (IgniteCache<Integer, Object> cache = src.getOrCreateCache(ccfg)) {
                long time = System.currentTimeMillis();

                Date date = new Date(time);

                cache.put(1, date);

                assertEquals(date, cache.get(1));
            }
        }
    }
}
