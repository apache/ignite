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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class IgniteCachePartitionedAtomicColumnConstraintsTest extends GridCommonAbstractTest {
    /** */
    private static final long FUT_TIMEOUT = 10_000L;

    /** */
    private static final String STR_CACHE_NAME = "STR_STR";

    /** */
    private static final String STR_ORG_CACHE_NAME = "STR_ORG";
    
    private static final String STR_ORG_WITH_FIELDS_CACHE_NAME = "STR_ORG_WITH_FIELDS";

    /** */
    private static final String OBJ_CACHE_NAME = "ORG_ADDRESS";

    /** */
    private Consumer<Runnable> shouldFail = (op) -> assertThrowsWithCause(op, IgniteException.class);

    /** */
    private Consumer<Runnable> shouldSucceed = Runnable::run;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        Map<String, Integer> strStrPrecision = new HashMap<>();

        strStrPrecision.put(KEY_FIELD_NAME, 5);
        strStrPrecision.put(VAL_FIELD_NAME, 5);

        jcache(grid(0), cacheConfiguration(new QueryEntity(String.class.getName(), String.class.getName())
            .setFieldsPrecision(strStrPrecision)), STR_CACHE_NAME);

        Map<String, Integer> orgAddressPrecision = new HashMap<>();

        orgAddressPrecision.put("name", 5);
        orgAddressPrecision.put("address", 5);

        jcache(grid(0), cacheConfiguration(new QueryEntity(Organization.class.getName(), Address.class.getName())
            .addQueryField("name", "java.lang.String", "name")
            .addQueryField("address", "java.lang.String", "address")
            .setFieldsPrecision(orgAddressPrecision)), OBJ_CACHE_NAME);

        Map<String, Integer> strOrgPrecision = new HashMap<>();

        strOrgPrecision.put(KEY_FIELD_NAME, 5);

        jcache(grid(0), cacheConfiguration(new QueryEntity(String.class.getName(), Organization.class.getName())
            .setFieldsPrecision(strOrgPrecision)), STR_ORG_CACHE_NAME);

        jcache(grid(0), cacheConfiguration(new QueryEntity(String.class.getName(), Organization.class.getName())
            .addQueryField("name", "java.lang.String", "name")
            .addQueryField("address", "java.lang.String", "address")
            .setFieldsPrecision(strOrgPrecision)), STR_ORG_WITH_FIELDS_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValueFail() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        T2<String, String> val = new T2<>("3", "123456");

        checkPutAll(shouldFail, cache, new T2<>("1", "1"), val);

        checkPutOps(shouldFail, cache, val);
        
        checkReplaceOps(shouldFail, cache, val, "1");
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFail() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        T2<String, String> val = new T2<>("123456", "2");

        checkPutAll(shouldFail, cache, new T2<>("1", "1"), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValueFieldFail() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        T2<Organization, Address> val = new T2<>(new Organization("3"), new Address("123456"));

        checkPutAll(shouldFail, cache, new T2<>(new Organization("1"), new Address("1")), val);

        checkPutOps(shouldFail, cache, val);

        checkReplaceOps(shouldFail, cache, val, new Address("1"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFieldFail() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        T2<Organization, Address> val = new T2<>(new Organization("123456"), new Address("2"));

        checkPutAll(shouldFail, cache, new T2<>(new Organization("1"), new Address("1")), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFail2() throws Exception {
        doCheckPutTooLongKeyFail2(STR_ORG_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFail3() throws Exception {
        doCheckPutTooLongKeyFail2(STR_ORG_WITH_FIELDS_CACHE_NAME);
    }


    private void doCheckPutTooLongKeyFail2(String cacheName) {
        IgniteCache<String, Organization> cache = jcache(0, cacheName);

        T2<String, Organization> val = new T2<>("123456", new Organization("1"));

        checkPutAll(shouldFail, cache, new T2<>("1", new Organization("1")), val);

        checkPutOps(shouldFail, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutLongValue() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        T2<String, String> val = new T2<>("3", "12345");

        checkPutAll(shouldSucceed, cache, new T2<>("1", "1"), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, "1");
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutLongKey() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        T2<String, String> val = new T2<>("12345", "2");

        checkPutAll(shouldSucceed, cache, new T2<>("1", "1"), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutLongValueField() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        T2<Organization, Address> val = new T2<>(new Organization("3"), new Address("12345"));

        checkPutAll(shouldSucceed, cache, new T2<>(new Organization("1"), new Address("1")), val);

        checkPutOps(shouldSucceed, cache, val);

        checkReplaceOps(shouldSucceed, cache, val, new Address("1"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutLongKeyField() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        T2<Organization, Address> val = new T2<>(new Organization("12345"), new Address("2"));

        checkPutAll(shouldSucceed, cache, new T2<>(new Organization("1"), new Address("1")), val);

        checkPutOps(shouldSucceed, cache, val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutLongKey2() throws Exception {
        doCheckPutLongKey2(STR_ORG_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutLongKey3() throws Exception {
        doCheckPutLongKey2(STR_ORG_WITH_FIELDS_CACHE_NAME);
    }

    private void doCheckPutLongKey2(String cacheName) {
        IgniteCache<String, Organization> cache = jcache(0, cacheName);

        T2<String, Organization> key2 = new T2<>("12345", new Organization("1"));

        checkPutAll(shouldSucceed, cache, new T2<>("1", new Organization("1")), key2);

        checkPutOps(shouldSucceed, cache, key2);
    }

    /** */
    private <K, V> void checkReplaceOps(Consumer<Runnable> checker, IgniteCache<K, V> cache, T2<K, V> val, V okVal) {
        K k = val.get1();
        V v = val.get2();

        cache.put(k, okVal);

        CacheEntryProcessor<K, V, ?> entryProcessor = (e, arguments) -> {
            e.setValue((V)arguments[0]);

            return null;
        };

        Stream<Runnable> ops = Stream.of(
            () -> cache.replace(k, v),
            () -> cache.getAndReplace(k, v),
            () -> cache.replace(k, okVal, v),
            () -> cache.invoke(k, entryProcessor, v),
            () -> cache.replaceAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.getAndReplaceAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.replaceAsync(k, okVal, v).get(FUT_TIMEOUT),
            () -> cache.invokeAsync(k, entryProcessor, v).get(FUT_TIMEOUT)
        );

        ops.forEach(checker);
    }

    /** */
    private <K, V> void checkPutOps(Consumer<Runnable> checker, IgniteCache<K, V> cache, T2<K, V> val) {
        K k = val.get1();
        V v = val.get2();

        Stream<Runnable> ops = Stream.of(
            () -> cache.put(k, v),
            () -> cache.putIfAbsent(k, v),
            () -> cache.getAndPut(k, v),
            () -> cache.getAndPutIfAbsent(k, v),
            () -> cache.putAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.putIfAbsentAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.getAndPutAsync(k, v).get(FUT_TIMEOUT),
            () -> cache.getAndPutIfAbsentAsync(k, v).get(FUT_TIMEOUT)
        );

        ops.forEach(checker);
    }

    /** */
    private <K, V> void checkPutAll(Consumer<Runnable> checker, IgniteCache<K, V> cache, T2<K, V>... entries) {
        CacheEntryProcessor<K, V, ?> entryProcessor = (e, arguments) -> {
            e.setValue(((Iterator<V>)arguments[0]).next());

            return null;
        };

        Map<K, V> vals = Arrays.stream(entries).collect(Collectors.toMap(T2::get1, T2::get2));

        Stream<Runnable> ops = Stream.of(
            () -> cache.putAll(vals),
            () -> cache.putAllAsync(vals).get(FUT_TIMEOUT),
            () -> {
                Map<K, ? extends EntryProcessorResult<?>> map =
                    cache.invokeAll(vals.keySet(), entryProcessor, vals.values().iterator());

                for (EntryProcessorResult<?> result : map.values())
                    log.info(">>> " + result.get());
            },
            () -> {
                Map<K, ? extends EntryProcessorResult<?>> map =
                    cache.invokeAllAsync(vals.keySet(), entryProcessor, vals.values().iterator()).get(FUT_TIMEOUT);

                for (EntryProcessorResult<?> result : map.values())
                    log.info(">>> " + result.get());
            }
        );

        ops.forEach(checker);
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
