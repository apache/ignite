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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractFieldsQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests for fields queries.
 */
public class IgniteCacheReplicatedFieldsQuerySelfTest extends IgniteCacheAbstractFieldsQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLostIterator() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(null);

        QueryCursor<List<?>> qry = null;

        int maximumQueryIteratorCount = GridCacheQueryManager.MAX_ITERATORS;

        for (int i = 0; i < maximumQueryIteratorCount + 1; i++) {
            QueryCursor<List<?>> q = cache
               .query(new SqlFieldsQuery("select _key from Integer where _key >= 0 order by _key"));

            assertEquals(0, q.iterator().next().get(0));

            if (qry == null)
                qry = q;
        }

        final QueryCursor<List<?>> qry0 = qry;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                int i = 0;

                for (List<?> row : qry0)
                    assertEquals(++i % 50, row.get(0));

                return null;
            }
        }, IgniteException.class, null);
    }
}