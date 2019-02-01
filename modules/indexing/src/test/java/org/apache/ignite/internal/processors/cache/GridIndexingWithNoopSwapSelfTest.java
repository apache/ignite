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

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest.ObjectValue;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * GG-4368
 */
public class GridIndexingWithNoopSwapSelfTest extends GridCommonAbstractTest {
    /** */
    protected Ignite ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?,?> cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setRebalanceMode(SYNC);
        cc.setNearConfiguration(new NearCacheConfiguration());

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(1000);

        cc.setEvictionPolicy(plc);
        cc.setOnheapCacheEnabled(true);
        cc.setBackups(1);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setIndexedTypes(
            Integer.class, ObjectValue.class
        );

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        ignite = null;
    }

    /** @throws Exception If failed. */
    @Test
    public void testQuery() throws Exception {
        IgniteCache<Integer, ObjectValue> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int cnt = 10;

        for (int i = 0; i < cnt; i++)
            cache.getAndPut(i, new ObjectValue("test" + i, i));

        for (int i = 0; i < cnt; i++) {
            assertNotNull(cache.localPeek(i, ONHEAP_PEEK_MODES));

            cache.localEvict(Collections.singleton(i)); // Swap.
        }

        SqlQuery<Integer, ObjectValue> qry =
            new SqlQuery(ObjectValue.class, "intVal >= ? order by intVal");

        assertEquals(10, cache.query(qry.setArgs(0)).getAll().size());
    }
}
