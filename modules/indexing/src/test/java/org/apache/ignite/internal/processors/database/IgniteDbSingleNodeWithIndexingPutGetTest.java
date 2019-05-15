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

package org.apache.ignite.internal.processors.database;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.junit.Test;

/**
 *
 */
public class IgniteDbSingleNodeWithIndexingPutGetTest extends IgniteDbSingleNodePutGetTest {
    /** {@inheritDoc} */
    @Override protected boolean indexingEnabled() {
        return true;
    }

    /**
     */
    @Test
    public void testGroupIndexes() {
        IgniteEx ig = grid(0);

        IgniteCache<Integer, Abc> cache = ig.cache("abc");

        long cnt = 50_000;

        for (int i = 0; i < cnt; i++)
            cache.put(i, new Abc(i, i % 10, i % 100));

        assertEquals(1, querySize(cache, "select count(*) from Abc"));
        assertEquals(Long.valueOf(cnt), queryOne(cache, "select count(*) from Abc"));

        assertEquals((int)cnt, querySize(cache, "select count(*) from Abc group by a"));
        assertEquals(Long.valueOf(1L), queryOne(cache, "select count(*) from Abc group by a"));
        assertEquals(Long.valueOf(1L), queryOne(cache, "select count(*) from Abc where a = 1"));

        assertEquals(10, querySize(cache, "select count(*) from Abc group by b"));
        assertEquals(Long.valueOf(cnt / 10), queryOne(cache, "select count(*) from Abc group by b"));
        assertEquals(Long.valueOf(cnt / 10), queryOne(cache, "select count(*) from Abc where b = 1"));

        assertEquals(100, querySize(cache, "select count(*) from Abc group by c"));
        assertEquals(Long.valueOf(cnt / 100), queryOne(cache, "select count(*) from Abc group by c"));
        assertEquals(Long.valueOf(cnt / 100), queryOne(cache, "select count(*) from Abc where c = 1"));
    }

    /**
     */
    @Test
    public void testGroupIndexes2() {
        IgniteEx ig = grid(0);

        IgniteCache<Integer,Abc> cache = ig.cache("abc");

        long cnt = 10_000;

        Map<Integer,AtomicLong> as = new TreeMap<>();
        Map<Integer,AtomicLong> bs = new TreeMap<>();
        Map<Integer,AtomicLong> cs = new TreeMap<>();

        Random rnd = ThreadLocalRandom.current();

        for (int i = 0; i < cnt; i++) {
            Abc abc = new Abc(rnd.nextInt(2000), rnd.nextInt(100), rnd.nextInt(5));

//            X.println(">> " + i + " " + abc);

            cache.put(i, abc);

            add(as, abc.a, true);
            add(bs, abc.b, true);
            add(cs, abc.c, true);

            if (rnd.nextInt(1000) == 0) {
                switch (rnd.nextInt(3)) {
                    case 0:
                        check(as, cache, "a");

                        break;

                    case 1:
                        check(bs, cache, "b");

                        break;

                    case 2:
                        check(cs, cache, "c");

                        break;

                    default:
                        fail();
                }
            }
        }

        check(as, cache, "a");
        check(bs, cache, "b");
        check(cs, cache, "c");
    }

    /**
     * @param xs Counters.
     * @param cache Cache.
     * @param field Field name.
     */
    private static void check(Map<Integer,AtomicLong> xs, IgniteCache<Integer,Abc> cache, String field) {
        String qry = "select " + field + ", count(*) from Abc group by " + field + " order by " + field;

        List<List<?>> res = cache.query(new SqlFieldsQuery(qry)).getAll();

        assertEquals(xs.size(), res.size());

        int i = 0;

        for (Map.Entry<Integer,AtomicLong> entry : xs.entrySet()) {
//            X.println("    " + field + ": " + entry);

            int key = entry.getKey();
            long cnt = entry.getValue().get();

            assertEquals(key, res.get(i).get(0));
            assertEquals(cnt, res.get(i).get(1));

            qry = "select 1 from Abc where " + field + " = " + key;
            assertEquals(cnt, querySize(cache, qry));

            qry = "select count(*) from Abc where " + field + " = " + key;
            assertEquals(Long.valueOf(cnt), queryOne(cache, qry));

            i++;
        }
    }

    /**
     * @param xs Map.
     * @param key Key.
     * @param inc Increment.
     */
    private static void add(Map<Integer,AtomicLong> xs, int key, boolean inc) {
        AtomicLong cntr = xs.get(key);

        if (cntr == null) {
            if (!inc)
                fail("Nothing to decrement.");

            xs.put(key, cntr = new AtomicLong());
        }

        cntr.addAndGet(inc ? 1 : -1);
    }

    @SuppressWarnings("unchecked")
    private static <X> X queryOne(IgniteCache<?, ?> cache, String qry) {
        return (X)cache.query(new SqlFieldsQuery(qry)).getAll().get(0).get(0);
    }

    private static int querySize(IgniteCache<?, ?> cache, String qry) {
        return cache.query(new SqlFieldsQuery(qry)).getAll().size();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration("abc");

        if (indexingEnabled())
            ccfg.setIndexedTypes(Integer.class, Abc.class);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        cfg.setCacheConfiguration(F.concat(cfg.getCacheConfiguration(), ccfg));

        return cfg;
    }

    static class Abc {
        /** */
        @QuerySqlField
//            (index = true)
            (orderedGroups = {
                @QuerySqlField.Group(name = "abc", order = 0),
            })
        private int a;

        /** */
        @QuerySqlField
            (index = true, orderedGroups = {
                @QuerySqlField.Group(name = "abc", order = 1),
                @QuerySqlField.Group(name = "cb", order = 1)
            })
        private int b;

        /** */
        @QuerySqlField
//            (index = true)
            (orderedGroups = {
                @QuerySqlField.Group(name = "abc", order = 2),
                @QuerySqlField.Group(name = "cb", order = 0)
            })
        private int c;

        /**
         * @param a A.
         * @param b B.
         * @param c C.
         */
        Abc(int a, int b, int c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Abc.class, this);
        }
    }
}
