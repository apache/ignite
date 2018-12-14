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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class TableViewSubquerySelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_COUNT = 1;

    /** */
    private static Ignite ignite;

    /** */
    private static IgniteCache<?, ?> initCache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGridsMultiThreaded(NODES_COUNT, false);
        initCache = ignite.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setSqlSchema("PUBLIC")
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    public void testSubqueryTableView() {
        final String cacheName = "a1";

        final String creationQry = "CREATE TABLE t1 ( id INT NOT NULL, int_col1 INT NOT NULL, PRIMARY KEY (id)) " +
            "WITH \"TEMPLATE=partitioned, cache_name=%s\""; //, WRAP_VALUE=false

        try (FieldsQueryCursor<List<?>> cur = initCache.query(
            new SqlFieldsQuery(String.format(creationQry,cacheName)))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(0L, rows.get(0).get(0));
        }

        final IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheName);

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "INSERT INTO t1 (id,int_col1) VALUES (1,0),(2,2),(3,0),(4,2)"))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(1, rows.size());

            assertEquals(4L, rows.get(0).get(0));
        }

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "SELECT * FROM ( SELECT * FROM t1 WHERE int_col1 > 0 ORDER BY id ) WHERE int_col1 = 1"))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(0, rows.size());
        }

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "SELECT * FROM ( SELECT * FROM t1 WHERE int_col1 < 0 ORDER BY id ) WHERE int_col1 = 1"))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(0, rows.size());
        }

        try (FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery(
            "SELECT * FROM ( SELECT * FROM t1 WHERE int_col1 > 0 ORDER BY id ) WHERE int_col1 = 2"))) {
            assertNotNull(cur);

            List<List<?>> rows = cur.getAll();

            assertEquals(2, rows.size());

            assertEquals(2, rows.get(0).get(0));

            assertEquals(2, rows.get(0).get(1));

            assertEquals(4, rows.get(1).get(0));

            assertEquals(2, rows.get(1).get(1));
        }
    }
}
