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
import org.junit.Test;

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
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = super.cacheConfiguration();

        cc.setNearConfiguration(nearConfiguration());

        return cc;
    }

    /** @throws Exception If failed. */
    @Test
    public void testLocalQuery() throws Exception {
        doTestLocalQuery(intCache, new SqlFieldsQuery("select _key, _val from Integer"));
    }

    /** @throws Exception If failed. */
    @Test
    public void testLocalQueryNoOpCache() throws Exception {
        doTestLocalQuery(noOpCache, new SqlFieldsQuery("select _key, _val from \"Integer-Integer\".Integer"));
    }

    /**
     * Execute given query locally and check results.
     * @param cache Cache to run query on.
     * @param fldsQry Query.
     */
    private void doTestLocalQuery(IgniteCache<?, ?> cache, SqlFieldsQuery fldsQry) throws InterruptedException {
        awaitPartitionMapExchange(true, true, null);

        int exp = 0;

        for (Cache.Entry e: intCache.localEntries(CachePeekMode.PRIMARY)) {
            if (e.getValue() instanceof Integer)
                exp++;
        }

        QueryCursor<List<?>> qry = cache.query(fldsQry.setLocal(true));

        assertEquals(exp, qry.getAll().size());
    }
}
