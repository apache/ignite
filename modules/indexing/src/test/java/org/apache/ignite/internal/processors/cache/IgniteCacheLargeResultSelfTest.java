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

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 */
public class IgniteCacheLargeResultSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?,?> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setBackups(1);
        cacheCfg.setIndexedTypes(
            Integer.class, Integer.class
        );

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);
    }

    /**
     */
    @Test
    public void testLargeResult() {
        // Fill cache.
        try (IgniteDataStreamer<Integer, Integer> streamer = ignite(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.perNodeBufferSize(20000);

            for (int i = 0; i < 50_000; i++)  // default max merge table size is 10000
                streamer.addData(i, i);

            streamer.flush();
        }

        IgniteCache<Integer, Integer> cache = ignite(0).cache(DEFAULT_CACHE_NAME);

        try (QueryCursor<List<?>> res = cache.query(
            new SqlFieldsQuery("select _val from Integer where _key between ? and ?")
                .setArgs(10_000, 40_000))) {

            int cnt = 0;

            for (List<?> row : res) {
                cnt++;

                int val = (Integer)row.get(0);

                assertTrue(val >= 10_000 && val <= 40_000);
            }

            assertEquals(30_001, cnt); // Streaming of a large result works well.
        }

        // Currently we have no ways to do multiple passes through a merge table.
    }
}
