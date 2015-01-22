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

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.cache.*;

import java.util.*;

import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Tests local query.
 */
public class GridCacheLocalQuerySelfTest extends GridCacheAbstractQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return LOCAL;
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testQueryLocal() throws Exception {
        GridCache<Integer, String> cache = ignite.cache(null);

        cache.put(1, "value1");
        cache.put(2, "value2");
        cache.put(3, "value3");
        cache.put(4, "value4");
        cache.put(5, "value5");

        // Tests equals query.
        GridCacheQuery<Map.Entry<Integer, String>> qry = cache.queries().createSqlQuery(String.class, "_val='value1'");

        GridCacheQueryFuture<Map.Entry<Integer,String>> iter = qry.execute();

        Map.Entry<Integer, String> entry = iter.next();

        assert iter.next() == null;

        assert entry != null;
        assert entry.getKey() == 1;
        assert "value1".equals(entry.getValue());

        // Tests like query.
        qry = cache.queries().createSqlQuery(String.class, "_val like 'value%'");

        iter = qry.execute();

        assert iter.next() != null;
        assert iter.next() != null;
        assert iter.next() != null;
        assert iter.next() != null;
        assert iter.next() != null;
        assert iter.next() == null;

        // Tests reducer.
        GridCacheQuery<Map.Entry<Integer, String>> rdcQry = cache.queries().createSqlQuery(String.class,
            "_val like 'value%' and _key != 2 and _val != 'value3' order by _val");

        Iterator<String> iter2 = rdcQry.
            projection(ignite.cluster().forLocal()).
            execute(new IgniteReducer<Map.Entry<Integer, String>, String>() {
                /** */
                private String res = "";

                @Override public boolean collect(Map.Entry<Integer, String> e) {
                    res += e.getValue();

                    return true;
                }

                @Override public String reduce() {
                    return res;
                }
            }).get().iterator();

        String res = iter2.next();

        assert res != null;
        assert "value1value4value5".equals(res);
    }
}
