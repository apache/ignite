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

package org.apache.ignite.internal.processors.cache.local;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Tests local query.
 */
public class IgniteCacheLocalQuerySelfTest extends IgniteCacheAbstractQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return LOCAL;
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testQueryLocal() throws Exception {
        // Let's do it twice to see how prepared statement caching behaves - without recompilation
        // check for cached prepared statements this would fail.
        for (int i = 0; i < 2; i++) {
            IgniteCache<Integer, String> cache = jcache(Integer.class, String.class);

            cache.put(1, "value1");
            cache.put(2, "value2");
            cache.put(3, "value3");
            cache.put(4, "value4");
            cache.put(5, "value5");

            // Tests equals query.
            QueryCursor<Cache.Entry<Integer, String>> qry =
                cache.query(new SqlQuery<Integer, String>(String.class, "_val='value1'").setLocal(true));

            Iterator<Cache.Entry<Integer, String>> iter = qry.iterator();

            Cache.Entry<Integer, String> entry = iter.next();

            assert !iter.hasNext();

            assert entry != null;
            assert entry.getKey() == 1;
            assert "value1".equals(entry.getValue());

            // Tests like query.
            qry = cache.query(new SqlQuery<Integer, String>(String.class, "_val like 'value%'").setLocal(true));

            iter = qry.iterator();

            assert iter.next() != null;
            assert iter.next() != null;
            assert iter.next() != null;
            assert iter.next() != null;
            assert iter.next() != null;
            assert !iter.hasNext();

            // Test explain for primitive index.
            List<List<?>> res = cache.query(new SqlFieldsQuery(
                    "explain select _key from String where _val > 'value1'").setLocal(true)).getAll();

            assertTrue("__ explain: \n" + res, ((String) res.get(0).get(0)).toLowerCase().contains("_val_idx"));

            cache.destroy();
        }
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLocalSqlQueryFromClient() throws Exception {
        try (Ignite g = startClientGrid("client")) {
            IgniteCache<Integer, Integer> c = jcache(g, Integer.class, Integer.class);

            for (int i = 0; i < 10; i++)
                c.put(i, i);

            SqlQuery<Integer, Integer> qry = new SqlQuery<>(Integer.class, "_key >= 5 order by _key");

            qry.setLocal(true);

            try (QueryCursor<Cache.Entry<Integer, Integer>> qryCursor = c.query(qry)) {
                assertNotNull(qryCursor);

                List<Cache.Entry<Integer, Integer>> res = qryCursor.getAll();

                assertNotNull(res);

                assertEquals(5, res.size());
            }
        }
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLocalSqlFieldsQueryFromClient() throws Exception {
        try (Ignite g = startClientGrid("client")) {
            IgniteCache<UUID, Person> c = jcache(g, UUID.class, Person.class);

            Person p = new Person("Jon", 1500);

            c.put(p.id(), p);

            SqlFieldsQuery qry = new SqlFieldsQuery("select * from Person");

            qry.setLocal(true);

            try (FieldsQueryCursor<List<?>> qryCursor = c.query(qry)) {
                assertNotNull(qryCursor);

                List<List<?>> res = qryCursor.getAll();

                assertNotNull(res);

                assertEquals(1, res.size());
            }
        }
    }
}
