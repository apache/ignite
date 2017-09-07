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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractFieldsQuerySelfTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests for fields queries.
 */
public class IgniteCachePartitionedFieldsQuerySelfTest extends IgniteCacheAbstractFieldsQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Distribution.
     */
    protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cache(@Nullable String name, boolean primitives) {
        CacheConfiguration cc = super.cache(name, primitives);

        cc.setNearConfiguration(nearConfiguration());

        return cc;
    }

    /** @throws Exception If failed. */
    public void testLocalQuery() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache( null);

        awaitPartitionMapExchange(true, true, null);

        int expected = 0;

        for(Cache.Entry e: cache.localEntries(CachePeekMode.PRIMARY)){
            if(e.getValue() instanceof Integer)
                expected++;
        }

        QueryCursor<List<?>> qry = cache
            .query(new SqlFieldsQuery("select _key, _val from Integer").setLocal(true));

        assertEquals(expected, qry.getAll().size());
    }
}