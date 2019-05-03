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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test index behavior when key is of plain Object type per indexing settings.
 */
public class IgniteCacheObjectKeyIndexingSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        Ignition.stopAll(true);
    }

    /** */
    protected static CacheConfiguration<Object, TestObject> cacheCfg() {
        return new CacheConfiguration<Object, TestObject>(DEFAULT_CACHE_NAME)
            .setIndexedTypes(Object.class, TestObject.class);
    }

    /** */
    @Test
    public void testObjectKeyHandling() throws Exception {
        Ignite ignite = grid();

        IgniteCache<Object, TestObject> cache = ignite.getOrCreateCache(cacheCfg());

        UUID uid = UUID.randomUUID();

        cache.put(uid, new TestObject("A"));

        assertItemsNumber(1);

        cache.put(1, new TestObject("B"));

        assertItemsNumber(2);

        cache.put(uid, new TestObject("C"));

        // Key should have been replaced
        assertItemsNumber(2);

        List<List<?>> res = cache.query(new SqlFieldsQuery("select _key, name from TestObject order by name")).getAll();

        assertEquals(res,
            Arrays.asList(
                Arrays.asList(1, "B"),
                Arrays.asList(uid, "C")
            )
        );

        cache.remove(1);

        assertItemsNumber(1);

        res = cache.query(new SqlFieldsQuery("select _key, name from TestObject")).getAll();

        assertEquals(res,
            Collections.singletonList(
                Arrays.asList(uid, "C")
            )
        );

        cache.remove(uid);

        // Removal has worked for both keys although the table was the same and keys were of different type
        assertItemsNumber(0);
    }

    /** */
    @SuppressWarnings("ConstantConditions")
    private void assertItemsNumber(long num) {
        assertEquals(num, grid().cachex(DEFAULT_CACHE_NAME).size());

        assertEquals(num, grid().cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("select count(*) from TestObject")).getAll()
            .get(0).get(0));
    }

    /** */
    private static class TestObject {
        /** */
        @QuerySqlField
        public final String name;

        /** */
        private TestObject(String name) {
            this.name = name;
        }
    }
}
