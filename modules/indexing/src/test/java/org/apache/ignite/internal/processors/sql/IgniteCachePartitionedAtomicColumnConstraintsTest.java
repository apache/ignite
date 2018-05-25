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
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteFuture;
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

    /** */
    private static final String OBJ_CACHE_NAME = "ORG_ADDRESS";

    /** */
    private T2<String, String> tooLongKey = new T2<>("123456", "2");

    /** */
    private T2<String, String> tooLongVal = new T2<>("3", "123456");

    /** */
    private T2<String, Organization> tooLongKey2 = new T2<>("123456", new Organization("1"));

    /** */
    private T2<Organization, Address> tooLongAddrVal = new T2<>(new Organization("3"), new Address("123456"));

    /** */
    private T2<Organization, Address> tooLongOrgKey = new T2<>(new Organization("123456"), new Address("2"));

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

        Map<String, Integer> strOrgMaxLengthInfo = new HashMap<>();

        strOrgMaxLengthInfo.put(KEY_FIELD_NAME, 5);

        jcache(grid(0), cacheConfiguration(new QueryEntity(String.class.getName(), Organization.class.getName())
            .setMaxLengthInfo(strOrgMaxLengthInfo)), STR_ORG_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValueFail() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        checkOpsFail(
            putAllOp(cache, 
                Stream.of(new T2<>("1", "1"), tooLongVal)));

        checkOpsFail(
            ops(cache, tooLongVal));

        checkOpsFail(
            ops3arg(cache, tooLongVal.getKey(), "1", tooLongVal.getValue()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFail() throws Exception {
        IgniteCache<String, String> cache = jcache(0, STR_CACHE_NAME);

        checkOpsFail(
            putAllOp(cache, 
                Stream.of(new T2<>("1", "1"), tooLongKey)));

        checkOpsFail(
            ops(cache, tooLongKey));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValueFieldFail() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        checkOpsFail(
            putAllOp(cache, 
                Stream.of(new T2<>(new Organization("1"), new Address("1")), tooLongAddrVal)));

        checkOpsFail(
            ops(cache, tooLongAddrVal));

        checkOpsFail(
            ops3arg(cache, tooLongAddrVal.get1(), new Address("1"), tooLongAddrVal.get2()));

    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFieldFail() throws Exception {
        IgniteCache<Organization, Address> cache = jcache(0, OBJ_CACHE_NAME);

        checkOpsFail(
            putAllOp(cache,
                Stream.of(new T2<>(new Organization("1"), new Address("1")), tooLongOrgKey)));

        checkOpsFail(
            ops(cache, tooLongOrgKey));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyFail2() throws Exception {
        IgniteCache<String, Organization> cache = jcache(0, STR_ORG_CACHE_NAME);

        checkOpsFail(
            putAllOp(cache,
                Stream.of(new T2<>("1", new Organization("1")), tooLongKey2)));

        checkOpsFail(
            ops(cache, tooLongKey2));
    }

    /** */
    private void checkOpsFail(Stream<Callable<?>> ops) {
        ops.forEach(op -> assertThrowsWithCause(op, IgniteException.class));
    }

    /** */
    private <K, V> Stream<Callable<?>> ops(IgniteCache<K, V> cache, T2<K, V> val) {
        Stream<BiFunction<K, V, ?>> ops = Stream.of(
            (k, v) -> { cache.put(k, v); return 0; },
            cache::putIfAbsent,
            cache::getAndPut,
            cache::getAndPutIfAbsent
        );

        Stream<BiFunction<K, V, IgniteFuture<?>>> asyncOps = Stream.of(
            cache::putAsync,
            cache::putIfAbsentAsync,
            cache::getAndPutAsync,
            cache::getAndPutIfAbsentAsync
        );

        Stream<BiFunction<K, V, ?>> allOps = Stream.concat(
            ops,
            asyncOps.map(op -> op.andThen(f -> f.get(FUT_TIMEOUT)))
        );

        return allOps.map(f -> () -> f.apply(val.get1(), val.get2()));
    }

    /** */
    private <K, V> Stream<Callable<?>> ops3arg(IgniteCache<K, V> cache, K key, V okVal, V errVal) {
        Stream<BiFunction<K, V, ?>> ops = Stream.of(
            cache::replace,
            cache::getAndReplace
        );

        Stream<BiFunction<K, V, IgniteFuture<?>>> asyncOps = Stream.of(
            cache::replaceAsync,
            cache::getAndReplaceAsync
        );

        Stream<TriFunction<K, V, V, ?>> allOps = Stream.concat(
            ops,
            asyncOps.map(op -> op.andThen(f -> f.get(FUT_TIMEOUT)))
        ).map(f -> (k, ok, err) -> { cache.put(k, ok); f.apply(k, err); return 0; });

        Stream<TriFunction<K, V, V, ?>> triArgOps = Stream.of(
            (k, ok, err) -> { cache.put(k, ok); cache.replace(k, ok, err); return 0; },
            (k, ok, err) -> { cache.put(k, ok); cache.replaceAsync(k, ok, err).get(FUT_TIMEOUT); return 0; }
        );

        return Stream.concat(allOps, triArgOps)
            .map(f -> () -> f.apply(key, okVal, errVal));
    }

    /** */
    private <K, V>  Stream<Callable<?>> putAllOp(IgniteCache<K, V> cache, Stream<T2<K, V>> entries) {
        return Stream.of(() -> {
            cache.putAll(
                entries.collect(Collectors.toMap(T2::get1, T2::get2)));

            return 0;
        });
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

    @FunctionalInterface
    interface TriFunction<A,B,C,R> {
        R apply(A a, B b, C c);

        default <V> TriFunction<A, B, C, V> andThen(Function<? super R, ? extends V> after) {
            Objects.requireNonNull(after);

            return (A a, B b, C c) -> after.apply(apply(a, b, c));
        }
    } 
}
