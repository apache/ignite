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

package org.apache.ignite.compatibility.persistence;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Test;

// TODO
// 1. disable WAL, as if it is enabled than new index fill from WAL with put new values.
//      But for that test it must just to be started on previous persisted files.
// 2. "Select *" is bad query as does not ask inline columns for compare.
public class SqlBasicCompatibilityTest extends PersistenceBasicCompatibilityTest {
//
//    @Test
//    public void test_2_1_0() throws Exception {
//        doTestStartupWithOldVersion("2.1.0", true);
//    }
//
//    @Test
//    public void test_2_2_0() throws Exception {
//        doTestStartupWithOldVersion("2.2.0", true);
//    }
//
//    @Test
//    public void test_2_3_0() throws Exception {
//        doTestStartupWithOldVersion("2.3.0", true);
//    }
//
//    @Test
//    public void test_2_4_0() throws Exception {
//        doTestStartupWithOldVersion("2.4.0", true);
//    }
//
//    @Test
//    public void test_2_5_0() throws Exception {
//        doTestStartupWithOldVersion("2.5.0", true);
//    }
//
//    @Test
//    public void test_2_6_0() throws Exception {
//        doTestStartupWithOldVersion("2.6.0", true);
//    }

    @Test
    public void test_2_7_0() throws Exception {
        doTestStartupWithOldVersion("2.7.0", true);
    }

    @Test
    public void test_2_7_6() throws Exception {
        doTestStartupWithOldVersion("2.7.6", true);
    }

    @Test
    public void test_2_8_0() throws Exception {
        doTestStartupWithOldVersion("2.8.0", true);
    }

    @Test
    public void test_2_8_1() throws Exception {
        doTestStartupWithOldVersion("2.8.1", true);
    }

    @Test
    public void test_2_9_0() throws Exception {
        doTestStartupWithOldVersion("2.9.0", true);
    }

    @Test
    public void test_2_9_1() throws Exception {
        doTestStartupWithOldVersion("2.9.1", true);
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param igniteVer 3-digits version of ignite
     * @throws Exception If failed.
     */
    @Override protected void doTestStartupWithOldVersion(String igniteVer, boolean compactFooter) throws Exception {
        boolean prev = this.compactFooter;

        try {
            this.compactFooter = compactFooter;

            startGrid(1, igniteVer,
                new PersistenceBasicCompatibilityTest.ConfigurationClosure(compactFooter),
                new PostStartupClosure());

            stopAllGrids();

            Thread.sleep(100);

//            IgniteEx ignite = startGrid(1, "2.9.1",
//                new PersistenceBasicCompatibilityTest.ConfigurationClosure(compactFooter),
//                new PostStartupClosure2());

            IgniteEx ignite = startGrid(0);

            assertEquals(1, ignite.context().discovery().topologyVersion());

            ignite.active(true);

            Thread.sleep(100);

            validateResultingCacheData(ignite.cache(TEST_CACHE_NAME));
        }
        finally {
            stopAllGrids();

            this.compactFooter = prev;
        }
    }

    /**
     * @param cache to be filled by different keys and values. Results may be validated in {@link
     * #validateResultingCacheData(Cache)}.
     */
    public static void saveCacheData(IgniteCache<Object, Object> cache) {
        for (int i = 0; i < 1; i++)
            cache.put(i, new EntityValueValue(new EntityValue(i + 2)));
    }

    /**
     * Asserts cache contained all expected values as it was saved before.
     *
     * @param cache Cache  should be filled using {@link #saveCacheData(Cache)}.
     */
    public static void validateResultingCacheData(IgniteCache<Object, Object> cache) {
        List<List<?>> result = cache.query(
            new SqlFieldsQuery(
                "SELECT * FROM \"" + TEST_CACHE_NAME + "\".EntityValueValue v where v.value = ?")
            .setArgs(new EntityValue(2))
        ).getAll();

        System.out.println("RESULT: " + result);

        assertTrue(result.size() == 1);

        List<?> row = result.get(0);

//        assertTrue(row.get(0).equals(0));
        assertTrue(row.get(0).equals(new EntityValue(2)));
    }

    /** */
    public static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>();
            cacheCfg.setName(TEST_CACHE_NAME);
            cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg.setBackups(1);
//            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            cacheCfg.setIndexedTypes(Integer.class, EntityValueValue.class);

//            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
//            fields.put("key", EntityKey.class.getName());
//            fields.put("value", EntityValue.class.getName());
//
//            HashSet<String> keyFields = new HashSet<>();
//            keyFields.add("key");
//
//            cacheCfg.setQueryEntities(Collections.singletonList(
//                new QueryEntity(EntityKey.class, EntityValueValue.class)
//                    .setKeyFieldName("key")
//                    .setKeyFields(keyFields)
//                    .setKeyType(EntityKey.class.getName())
//                    .setFields(fields)
//                    .setTableName("TEST")
//                    .setIndexes(
//                        F.asList(new QueryIndex("value")))))
//                ;

            IgniteCache<Object, Object> cache = ignite.createCache(cacheCfg);

            saveCacheData(cache);

            ignite.active(false);

            try {
                Thread.sleep(1_000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /** */
    public static class PostStartupClosure2 implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            try {
                Thread.sleep(1000);
            } catch (Exception e) {

            }

            validateResultingCacheData(ignite.cache(TEST_CACHE_NAME));
        }
    }


    /** */
    public static class EntityKey {
        private int high;

        private int low;

        public EntityKey(int high, int low) {
            this.high = high;
            this.low = low;
        }

        @Override public String toString() {
            return "EK[high=" + high + "; low=" + low + "]";
        }

        @Override public int hashCode() {
            return high + low;
        }

        @Override public boolean equals(Object other) {
            EntityKey o = (EntityKey) other;

            return high == o.high && low == o.low;
        }
    }

    /** */
    public static class EntityValue {
        private int value;

        public EntityValue(int value) {
            this.value = value;
        }

        @Override public String toString() {
            return "EV[value=" + value + "]";
        }

        @Override public int hashCode() {
            return 1 + value;
        }

        @Override public boolean equals(Object other) {
            return value == ((EntityValue) other).value;
        }
    }

    public static class EntityValueValue {

        @QuerySqlField(index = true)
        private EntityValue value;

        public EntityValueValue(EntityValue value) {
            this.value = value;
        }

        @Override public String toString() {
            return "EVV[value=" + value + "]";
        }
    }
}
