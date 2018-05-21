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

package org.apache.ignite.internal.processors.sql;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.internal.util.lang.GridFunc.t;

/** */
public class IgniteCachePartitionedAtomicColumnConstraintsTest extends GridCommonAbstractTest {
    /** */
    private static final long FUT_TIMEOUT = 10_000L;

    /** */
    private static final String STR_CACHE_NAME = "STR_STR";

    /** */
    private static final String OBJ_CACHE_NAME = "ORG_ADDRESS";

    /** */
    private IgniteBiTuple<String, String> tooLongVal = t("3", "123456");

    /** */
    private IgniteBiTuple<String, String> tooLongKey = t("123456", "2");

    /** */
    private IgniteBiTuple<Organization, Address> tooLongVal2 = t(new Organization("3"), new Address("123456"));

    /** */
    private IgniteBiTuple<Organization, Address> tooLongKey2 = t(new Organization("123456"), new Address("2"));

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        Map<String, Integer> strStrMaxLengthInfo = new HashMap<>();

        strStrMaxLengthInfo.put(KEY_FIELD_NAME, 5);
        strStrMaxLengthInfo.put(VAL_FIELD_NAME, 5);

        jcache(grid(0), cacheConfiguration(new QueryEntity(String.class.getName(), String.class.getName())
            .setMaxLengthInfo(strStrMaxLengthInfo)), STR_CACHE_NAME);

        Map<String, Integer> orgAddressMaxLengthInfo = new HashMap<>();

        orgAddressMaxLengthInfo.put("name", 5);
        orgAddressMaxLengthInfo.put("address", 5);

        jcache(grid(0), cacheConfiguration(new QueryEntity(Organization.class.getName(), Address.class.getName())
                    .addQueryField("name", "java.lang.String", "name")
                    .addQueryField("address", "java.lang.String", "address")
                    .setMaxLengthInfo(orgAddressMaxLengthInfo)), OBJ_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllTooLongValue() throws Exception {
        Map<String, String> entries = new HashMap<>();

        entries.put("1", "1");
        entries.put(tooLongVal.getKey(), tooLongVal.getValue());

        putAll(jcache(0, STR_CACHE_NAME), entries);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllTooLongKey() throws Exception {
        Map<String, String> entries = new HashMap<>();

        entries.put("1", "1");
        entries.put(tooLongKey.getKey(), tooLongKey.getValue());

        putAll(jcache(0, STR_CACHE_NAME), entries);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllTooLongValueField() throws Exception {
        Map<Organization, Address> entries = new HashMap<>();

        entries.put(new Organization("1"), new Address("1"));
        entries.put(tooLongVal2.getKey(), tooLongVal2.getValue());

        putAll(jcache(0, OBJ_CACHE_NAME), entries);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllTooLongKeyField() throws Exception {
        Map<Organization, Address> entries = new HashMap<>();

        entries.put(new Organization("1"), new Address("1"));
        entries.put(tooLongKey2.getKey(), tooLongKey2.getValue());

        putAll(jcache(0, OBJ_CACHE_NAME), entries);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValueField() throws Exception {
        put(jcache(0, OBJ_CACHE_NAME), tooLongVal2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyField() throws Exception {
        put(jcache(0, OBJ_CACHE_NAME), tooLongKey2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValue() throws Exception {
        put(jcache(0, STR_CACHE_NAME), tooLongVal);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKey() throws Exception {
        put(jcache(0, STR_CACHE_NAME), tooLongKey);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentTooLongValueField() throws Exception {
        putIfAbsent(jcache(0, OBJ_CACHE_NAME), tooLongVal2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentTooLongKeyField() throws Exception {
        putIfAbsent(jcache(0, OBJ_CACHE_NAME), tooLongKey2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentTooLongValue() throws Exception {
        putIfAbsent(jcache(0, STR_CACHE_NAME), tooLongVal);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentTooLongKey() throws Exception {
        putIfAbsent(jcache(0, STR_CACHE_NAME), tooLongKey);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutTooLongValueField() throws Exception {
        getAndPut(jcache(0, OBJ_CACHE_NAME), tooLongVal2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutTooLongKeyField() throws Exception {
        getAndPut(jcache(0, OBJ_CACHE_NAME), tooLongKey2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutTooLongValue() throws Exception {
        getAndPut(jcache(0, STR_CACHE_NAME), tooLongVal);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutTooLongKey() throws Exception {
        getAndPut(jcache(0, STR_CACHE_NAME), tooLongKey);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutIfAbsentTooLongValueField() throws Exception {
        getAndPutIfAbsent(jcache(0, OBJ_CACHE_NAME), tooLongVal2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutIfAbsentTooLongKeyField() throws Exception {
        getAndPutIfAbsent(jcache(0, OBJ_CACHE_NAME), tooLongKey2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutIfAbsentTooLongValue() throws Exception {
        getAndPutIfAbsent(jcache(0, STR_CACHE_NAME), tooLongVal);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndPutIfAbsentTooLongKey() throws Exception {
        getAndPutIfAbsent(jcache(0, STR_CACHE_NAME), tooLongKey);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplaceTooLongValueField() throws Exception {
        replace(jcache(0, OBJ_CACHE_NAME), tooLongVal2.getKey(), new Address("1"), tooLongVal2.getValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplaceTooLongValue() throws Exception {
        replace(jcache(0, STR_CACHE_NAME), tooLongVal.getKey(), "1", tooLongVal.getValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndReplaceTooLongValueField() throws Exception {
        getAndReplace(jcache(0, OBJ_CACHE_NAME), tooLongVal2.getKey(), new Address("1"), 
            tooLongVal2.getValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndReplaceTooLongValue() throws Exception {
        getAndReplace(jcache(0, STR_CACHE_NAME), tooLongVal.getKey(), "1", tooLongVal.getValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplace3TooLongValueField() throws Exception {
        replace3(jcache(0, OBJ_CACHE_NAME), tooLongVal2.getKey(), new Address("1"), tooLongVal2.getValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplace3TooLongValue() throws Exception {
        replace3(jcache(0, STR_CACHE_NAME), tooLongVal.getKey(), "1", tooLongVal.getValue());
    }

    /** */
    private <K, V> void put(IgniteCache<K, V> cache, final Map.Entry<K, V> entry) {
        GridTestUtils.assertThrowsWithCause(() -> {
            if (async())
                cache.putAsync(entry.getKey(), entry.getValue()).get(FUT_TIMEOUT);
            else
                cache.put(entry.getKey(), entry.getValue());

            return 0;
        }, IgniteException.class);
    }

    /** */
    private <K, V> void putIfAbsent(IgniteCache<K, V> cache, final Map.Entry<K, V> entry) {
        GridTestUtils.assertThrowsWithCause(() -> {
            if (async())
                cache.putIfAbsentAsync(entry.getKey(), entry.getValue()).get(FUT_TIMEOUT);
            else
                cache.putIfAbsent(entry.getKey(), entry.getValue());

            return 0;
        }, IgniteException.class);
    }

    /** */
    private <K, V> void getAndPut(IgniteCache<K, V> cache, final Map.Entry<K, V> entry) {
        GridTestUtils.assertThrowsWithCause(() -> {
            if (async())
                cache.getAndPutAsync(entry.getKey(), entry.getValue()).get(FUT_TIMEOUT);
            else
                cache.getAndPut(entry.getKey(), entry.getValue());

            return 0;
        }, IgniteException.class);
    }

    /** */
    private <K, V> void getAndPutIfAbsent(IgniteCache<K, V> cache, final Map.Entry<K, V> entry) {
        GridTestUtils.assertThrowsWithCause(() -> {
            if (async())
                cache.getAndPutIfAbsentAsync(entry.getKey(), entry.getValue()).get(FUT_TIMEOUT);
            else
                cache.getAndPutIfAbsent(entry.getKey(), entry.getValue());

            return 0;
        }, IgniteException.class);
    }

    /** */
    private <K, V> void replace(IgniteCache<K, V> cache, final K key, V okVal, V errorVal) {
        cache.put(key, okVal);

        GridTestUtils.assertThrowsWithCause(() -> {
            if (async())
                cache.replaceAsync(key, errorVal).get(FUT_TIMEOUT);
            else
                cache.replace(key, errorVal);

            return 0;
        }, IgniteException.class);
    }

    /** */
    private <K, V> void getAndReplace(IgniteCache<K, V> cache, final K key, V okVal, V errorVal) {
        cache.put(key, okVal);

        GridTestUtils.assertThrowsWithCause(() -> {
            if (async())
                cache.getAndReplaceAsync(key, errorVal).get(FUT_TIMEOUT);
            else
                cache.getAndReplace(key, errorVal);

            return 0;
        }, IgniteException.class);
    }

    /** */
    private <K, V> void replace3(IgniteCache<K, V> cache, final K key, V okVal, V errorVal) {
        cache.put(key, okVal);

        GridTestUtils.assertThrowsWithCause(() -> {
            if (async())
                cache.replaceAsync(key, okVal, errorVal).get(FUT_TIMEOUT);
            else
                cache.replace(key, okVal, errorVal);

            return 0;
        }, IgniteException.class);
    }

    /** */
    private <K, V> void putAll(IgniteCache<K, V> cache, final Map<K, V> entries) {
        GridTestUtils.assertThrowsWithCause(() -> {
            if (async())
                cache.putAllAsync(entries).get(FUT_TIMEOUT);
            else
                cache.putAll(entries);

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param qryEntity Query entity.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(QueryEntity qryEntity) {
        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

        cache.setCacheMode(cacheMode());
        cache.setAtomicityMode(atomicityMode());
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        cache.setQueryEntities(Collections.singletonList(qryEntity));

        return cache;
    }

    /** */
    @NotNull protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** */
    @NotNull protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** */
    protected boolean async() {
        return false;
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    private static class Organization implements Serializable {
        /** Name. */
        private final String name;

        /**
         * @param name Name.
         */
        private Organization(String name) {
            this.name = name;
        }
    }

    /** */
    @SuppressWarnings("UnusedDeclaration")
    private static class Address implements Serializable {
        /** Name. */
        private final String address;

        /**
         * @param address Address.
         */
        private Address(String address) {
            this.address = address;
        }
    }
}
