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

import java.util.Collection;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests replicated local query from client node.
 */
public class IgniteCacheReplicatedClientLocalQuerySelfTest extends IgniteCacheAbstractQuerySelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testClientsLocalQuery() throws Exception {
        try {
            Ignite g = startGrid("client");

            IgniteCache<Integer, Integer> c = jcache(g, Integer.class, Integer.class);

            for (int i = 0; i < 10; i++)
                c.put(i, i);

            // Client cache should be empty.
            assertEquals(0, c.localSize());

            SqlQuery<Integer, Integer> qry = new SqlQuery<>(Integer.class, "_key >= 5 order by _key");

            qry.setLocal(true); // NPE cause

            Collection<Cache.Entry<Integer, Integer>> res = c.query(qry).getAll();

            assertEquals(5, res.size());

            int i = 5;

            for (Cache.Entry<Integer, Integer> e : res) {
                assertEquals(i, e.getKey().intValue());
                assertEquals(i, e.getValue().intValue());

                i++;
            }
        }
        finally {
            stopGrid("client");
        }
    }

    @Override protected int gridCount() {
        return 1;
    }

    @Override protected CacheMode cacheMode() {
        return REPLICATED;
    }
}
