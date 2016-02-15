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

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;

/**
 *
 */
public class CacheQueryOffheapEvictDataLostTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS = 100_000;

    /**
     *
     */
    public CacheQueryOffheapEvictDataLostTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>();

        ccfg1.setName("cache-1");
        ccfg1.setMemoryMode(OFFHEAP_TIERED);
        ccfg1.setOffHeapMaxMemory(1024);
        ccfg1.setIndexedTypes(Integer.class, TestData.class);
        ccfg1.setSwapEnabled(false);

        CacheConfiguration<Object, Object> ccfg2 = new CacheConfiguration<>();

        ccfg2.setName("cache-2");
        ccfg2.setMemoryMode(ONHEAP_TIERED);
        ccfg2.setEvictionPolicy(new LruEvictionPolicy(10));
        ccfg2.setOffHeapMaxMemory(1024);
        ccfg2.setIndexedTypes(Integer.class, TestData.class);
        ccfg2.setSwapEnabled(false);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryDataLost() throws Exception {
        final long stopTime = U.currentTimeMillis() + 30_000;

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            void putGet(IgniteCache<Object, Object> cache) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = 0; i < KEYS; i++) {
                    cache.put(rnd.nextInt(KEYS), new TestData(i));

                    cache.get(rnd.nextInt(KEYS));
                }
            }

            void query(IgniteCache<Object, Object> cache) {
                SqlQuery<Object, Object> qry1 = new SqlQuery<>(TestData.class, "_key > ?");
                qry1.setArgs(KEYS / 2);

                cache.query(qry1).getAll();

                SqlQuery<Object, Object> qry2 = new SqlQuery<>(TestData.class, "idxVal > ?");
                qry2.setArgs(KEYS / 2);

                cache.query(qry2).getAll();
            }

            @Override public void apply(Integer idx) {
                IgniteCache<Object, Object> cache1 = grid().cache("cache-1");
                IgniteCache<Object, Object> cache2 = grid().cache("cache-2");

                while (U.currentTimeMillis() < stopTime) {
                    if (idx == 0) {
                        putGet(cache1);
                        putGet(cache2);
                    }
                    else {
                        query(cache1);
                        query(cache2);
                    }
                }
            }
        }, 10, "test-thread");
    }

    /**
     *
     */
    static class TestData implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private int idxVal;

        /**
         * @param idxVal Value.
         */
        public TestData(int idxVal) {
            this.idxVal = idxVal;
        }
    }
}
