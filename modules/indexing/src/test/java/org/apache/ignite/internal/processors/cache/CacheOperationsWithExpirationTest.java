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
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 *
 */
public class CacheOperationsWithExpirationTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS = 10_000;

    /**
     * @param atomicityMode Atomicity mode.
     * @param idx Indexing enabled flag.
     * @return Cache configuration.
     */
    private CacheConfiguration<String, TestIndexedType> cacheConfiguration(CacheAtomicityMode atomicityMode,
        boolean idx) {
        CacheConfiguration<String, TestIndexedType> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        ccfg.setStatisticsEnabled(true);

        if (idx)
            ccfg.setIndexedTypes(String.class, TestIndexedType.class);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicIndexEnabled() throws Exception {
        concurrentPutGetRemoveExpireAndQuery(cacheConfiguration(ATOMIC, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        concurrentPutGetRemoveExpireAndQuery(cacheConfiguration(ATOMIC, false));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void concurrentPutGetRemoveExpireAndQuery(CacheConfiguration<String, TestIndexedType> ccfg)
        throws Exception {
        Ignite ignite = ignite(0);

        final IgniteCache<String, TestIndexedType> cache = ignite.createCache(ccfg);

        final boolean qryEnabled = !F.isEmpty(ccfg.getQueryEntities());

        try {
            final long stopTime = U.currentTimeMillis() + 30_000;

            GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
                @Override public void apply(Integer idx) {
                    while (U.currentTimeMillis() < stopTime) {
                        if (!qryEnabled || idx % 2 == 0)
                            putGet(cache);
                        else
                            query(cache);
                    }
                }

                void putGet(IgniteCache<String, TestIndexedType> cache) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    cache = cache.withExpiryPolicy(new ModifiedExpiryPolicy(
                        new Duration(MILLISECONDS, rnd.nextLong(100) + 1)));

                    for (int i = 0; i < KEYS; i++) {
                        String key = String.valueOf(rnd.nextInt(KEYS));

                        cache.put(key, testValue(rnd));
                    }

                    Set<String> s = new TreeSet<>();

                    for (int i = 0; i < 1000; i++) {
                        String key = String.valueOf(rnd.nextInt(KEYS));

                        s.add(key);
                    }

                    cache.getAll(s);

                    cache.removeAll(s);
                }

                void query(IgniteCache<String, TestIndexedType> cache) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    String k = String.valueOf(rnd.nextInt(KEYS));

                    SqlFieldsQuery qry1;

                    if (rnd.nextBoolean()) {
                        qry1 = new SqlFieldsQuery("select _key, _val from TestIndexedType where key1=? and intVal=?");

                        qry1.setArgs(k, k);
                    }
                    else
                        qry1 = new SqlFieldsQuery("select _key, _val from TestIndexedType");

                    List res = cache.query(qry1).getAll();

                    assertNotNull(res);
                }
            }, 10, "test-thread");
        }
        finally {
            ignite.destroyCache(cache.getName());
        }
    }

    /**
     * @param rnd Random.
     * @return Test value.
     */
    private TestIndexedType testValue(ThreadLocalRandom rnd) {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < rnd.nextInt(100); i++)
            builder.append("string");

        return new TestIndexedType(rnd.nextInt(KEYS), builder.toString());
    }

    /**
     *
     */
    public enum EnumType1 {
        /** */
        TYPE1,
        /** */
        TYPE2,
        /** */
        TYPE3;

        /** */
        static final EnumType1[] vals = EnumType1.values();
    }

    /**
     *
     */
    public enum EnumType2 {
        /** */
        TYPE1,
        /** */
        TYPE2,
        /** */
        TYPE3,
        /** */
        TYPE4;

        /** */
        static final EnumType2[] vals = EnumType2.values();
    }

    /**
     *
     */
    public static class TestIndexedType implements Serializable {
        /** */
        private static final long serialVersionUID = 1L;

        /** */
        @QuerySqlField
        private final String key;

        /** */
        @QuerySqlField(index = true)
        private final String key1;

        /** */
        @QuerySqlField(index = true)
        private final String key2;

        /** */
        @QuerySqlField(index = true)
        private final String key3;

        /** */
        @QuerySqlField(index = true)
        private final int intVal;

        /** */
        private final EnumType1 type1;

        /** */
        @QuerySqlField(index = true)
        private final EnumType2 type2;

        /** */
        @QuerySqlField(index = true)
        private final Date date1;

        /** */
        @QuerySqlField(index = true)
        private final Date date2;

        /** */
        private final Byte byteVal1;

        /** */
        private final Byte byteVal2;

        /**
         * @param rnd Random value.
         * @param strVal Random string value.
         */
        public TestIndexedType(int rnd, String strVal) {
            intVal = rnd;
            key = String.valueOf(rnd);
            key1 = key;
            key2 = strVal;
            key3 = strVal;
            date1 = new Date(rnd);
            date2 = new Date(U.currentTimeMillis());
            type1 = EnumType1.vals[rnd % EnumType1.vals.length];
            type2 = EnumType2.vals[rnd % EnumType2.vals.length];
            byteVal1 = (byte)rnd;
            byteVal2 = (byte)rnd;
        }
    }
}
