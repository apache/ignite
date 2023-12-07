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

import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class GridCacheFullTextQueryFailoverTest extends GridCacheFullTextQueryAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteCache<Integer, Person> cache = startGrids(2).cache(PERSON_CACHE);

        for (int i = 0; i < 100; i++)
            cache.put(i, new Person("str" + i));
    }

    /** */
    @Test
    public void testStopNodeDuringQuery() throws Exception {
        TextQuery<Integer, Person> qry = new TextQuery<Integer, Person>(Person.class, "str~")
            .setPageSize(10);

        Iterator<Cache.Entry<Integer, Person>> iter = cache().query(qry).iterator();

        // Initialize internal structures.
        iter.next();

        stopGrid(1);

        awaitPartitionMapExchange();

        GridTestUtils.assertThrows(log, iter::hasNext, CacheException.class, "Remote node has left topology");
    }

    /** */
    @Test
    public void testCancelQuery() {
        TextQuery<Integer, Person> qry = new TextQuery<Integer, Person>(Person.class, "str~")
            .setPageSize(10);

        QueryCursor<Cache.Entry<Integer, Person>> cursor = cache().query(qry);

        Iterator<Cache.Entry<Integer, Person>> iter = cursor.iterator();

        // Initialize internal structures.
        iter.next();

        cursor.close();

        assertFalse(iter.hasNext());

        GridTestUtils.assertThrows(log, iter::next, NoSuchElementException.class, null);
    }
}
