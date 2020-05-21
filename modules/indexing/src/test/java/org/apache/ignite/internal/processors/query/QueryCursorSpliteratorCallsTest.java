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

package org.apache.ignite.internal.processors.query;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Query cursor spliterator called multiple times without triggering IgniteException("Iterator is already fetched or
 * query was cancelled.")
 */
public class QueryCursorSpliteratorCallsTest extends GridCommonAbstractTest {
    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setIndexedTypes(Integer.class, String.class));
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteEx ignite = startGrids(1);
        IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

        cache.put(1, "11");
        cache.put(2, "12");
        cache.put(3, "13");
    }

    /**
     * @throws IgniteException If failed.
     */
    @Test
    public void testScanQueryCursorSpliteratorCalls() throws IgniteException {
        doQueryCursorSpliteratorCalls(new ScanQuery<>((key, val) -> key != null));
    }

    /**
     * @throws IgniteException If failed.
     */
    @Test
    public void testContinuousQueryCursorSpliteratorCalls() throws IgniteException {
        doQueryCursorSpliteratorCalls(new ContinuousQuery<>()
            .setInitialQuery(new ScanQuery<>((key, val) -> key != null))
            .setAutoUnsubscribe(true)
            .setLocalListener(iterable -> {
            }));
    }

    /**
     * @throws IgniteException If failed.
     */
    @Test
    public void testSqlQueryCursorSpliteratorCalls() throws IgniteException {
        doQueryCursorSpliteratorCalls(new SqlQuery<>("String", "from String"));
    }

    /**
     * @throws IgniteException If failed.
     */
    @Test
    public void testSqlFieldQueryCursorSpliteratorCalls() throws IgniteException {
        doQueryCursorSpliteratorCalls(new SqlFieldsQuery("select _key from String"));
    }

    /**
     * @throws IgniteException If failed.
     */
    @Test
    public void testTextQueryCursorSpliteratorCalls() throws IgniteException {
        doQueryCursorSpliteratorCalls(new TextQuery<>("String", "1?"));
    }

    /**
     * Executes query on cache then calls {@link QueryCursor#iterator()} and {@link QueryCursor#spliterator()}
     * sequentially.
     *
     * @param qry Query.
     */
    private void doQueryCursorSpliteratorCalls(Query<?> qry) {
        Ignite client = grid(0);

        IgniteCache<Object, String> cache = client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        Set<Object> iterData = new HashSet<>();
        Set<Object> spliterData = new HashSet<>();
        try (QueryCursor<?> cur = cache.query(qry)) {
            Iterator<Object> iter = (Iterator<Object>)cur.iterator();
            while (iter.hasNext())
                iterData.add(iter.next());
            assertEquals(iterData.size(), 3);
            Spliterator<Object> spliter = (Spliterator<Object>)cur.spliterator();
            assertEquals(spliter.getExactSizeIfKnown(), -1);

            GridTestUtils.assertThrows(log, IgniteException.class, "Iterator is already fetched or query was cancelled.",
                cur, "iterator");
        }
        try (QueryCursor<?> cur = cache.query(qry)) {
            Spliterator<Object> spliter = (Spliterator<Object>)cur.spliterator();
            assertEquals(spliter.getExactSizeIfKnown(), -1);
            spliter.forEachRemaining(spliterData::add);
            assertEquals(spliterData.size(), 3);
            Iterator<Object> iter = (Iterator<Object>)cur.iterator();
            assertFalse((qry instanceof ScanQuery) && iter.hasNext());

            GridTestUtils.assertThrows(log, IgniteException.class, "Iterator is already fetched or query was cancelled.",
                cur, "iterator");
        }
        assertEquals(iterData.size(), spliterData.size());
    }
}
