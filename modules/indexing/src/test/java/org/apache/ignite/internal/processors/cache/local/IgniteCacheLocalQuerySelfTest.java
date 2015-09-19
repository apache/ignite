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
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;

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
    public void testQueryLocal() throws Exception {
        IgniteCache<Integer, String> cache = ignite.cache(null);

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

        assertTrue("__ explain: \n" + res, ((String)res.get(0).get(0)).contains("_val_idx"));
    }
}