/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class AffinityAliasKeyTest extends GridCommonAbstractTest {
    /** */
    private static final String PERSON_CACHE = "PERSON";

    /** */
    private static IgniteEx ignite;

    /** */
    @Parameterized.Parameter
    public boolean sqlEscape;

    /** */
    @Parameterized.Parameters(name = "escape = {0}")
    public static Object[] params() {
        return new Object[] { false, true };
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        ignite.destroyCache(PERSON_CACHE);
    }

    /** */
    @Test
    public void testAliasAffinityKeyForIndexedTypes() {
        IgniteCache<PersonKey, Object> cache = ignite.createCache(new CacheConfiguration<PersonKey, Object>()
            .setName(PERSON_CACHE)
            .setSqlEscapeAll(sqlEscape)
            .setIndexedTypes(PersonKey.class, Object.class));

        checkAffinityColumnName(cache);
    }

    /** */
    private void checkAffinityColumnName(IgniteCache<PersonKey, Object> cache) {
        String expAffColName = sqlEscape ? "city_id" : "CITY_ID";

        try (FieldsQueryCursor<List<?>> cursor = cache.query(
            new SqlFieldsQuery("select AFFINITY_KEY_COLUMN from sys.tables where cache_name = '" + PERSON_CACHE + "'"))
        ) {
            List<List<?>> res = cursor.getAll();

            assertEquals(1, res.size());
            assertEquals(1, res.get(0).size());
            assertEquals(expAffColName, res.get(0).get(0));
        }
    }

    /** */
    public static class PersonKey {
        /** Field with alias set as an affinity key. */
        @QuerySqlField(name = "city_id")
        @AffinityKeyMapped
        private int cityId;
    }
}
