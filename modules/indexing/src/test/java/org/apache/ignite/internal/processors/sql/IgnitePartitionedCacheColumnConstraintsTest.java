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
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 */
public class IgnitePartitionedCacheColumnConstraintsTest extends GridCommonAbstractTest {
    public static final long FUT_TIMEOUT = 10_000L;
    
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteEx ignite = startGrid(0);

        Map<String, Integer> strStrMaxLengthInfo = new HashMap<>();

        strStrMaxLengthInfo.put("_KEY", 5);
        strStrMaxLengthInfo.put("_VALUE", 5);

        IgniteCache<String, String> strStrCache = jcache(grid(0),
            cacheConfiguration(
                new QueryEntity(String.class.getName(), String.class.getName())
                    .setMaxLengthInfo(strStrMaxLengthInfo)), "STR_STR");

        Map<String, Integer> orgAddressMaxLengthInfo = new HashMap<>();

        strStrMaxLengthInfo.put("name", 5);
        strStrMaxLengthInfo.put("address", 5);

        IgniteCache<Organization, Address> orgAddressCache = jcache(grid(0),
            cacheConfiguration(
                new QueryEntity(Organization.class.getName(), Address.class.getName())
                    .setMaxLengthInfo(orgAddressMaxLengthInfo)), "ORG_ADDRESS");
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValue() throws Exception {
        doPutTooLongValue(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKey() throws Exception {
        doPutTooLongKey(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongKeyField() throws Exception {
        doPutTooLongKeyField(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTooLongValueField() throws Exception {
        doPutTooLongValueField(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllTooLongValue() throws Exception {
        doPutAllTooLongValue(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllTooLongKey() throws Exception {
        doPutAllTooLongKey(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllTooLongKeyField() throws Exception {
        doPutAllTooLongKeyField(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllTooLongValueField() throws Exception {
        doPutAllTooLongValueField(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentTooLongValue() throws Exception {
        doPutIfAbsentTooLongValue(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentTooLongKey() throws Exception {
        doPutIfAbsentTooLongKey(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentTooLongKeyField() throws Exception {
        doPutIfAbsentTooLongKeyField(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentTooLongValueField() throws Exception {
        doPutIfAbsentTooLongValueField(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAsyncTooLongKey() throws Exception {
        doPutTooLongKey(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAsyncTooLongKeyField() throws Exception {
        doPutTooLongKeyField(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAsyncTooLongValueField() throws Exception {
        doPutTooLongValueField(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAsyncTooLongValue() throws Exception {
        doPutTooLongValue(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllAsyncTooLongValue() throws Exception {
        doPutAllTooLongValue(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllAsyncTooLongKey() throws Exception {
        doPutAllTooLongKey(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllAsyncTooLongKeyField() throws Exception {
        doPutAllTooLongKeyField(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllAsyncTooLongValueField() throws Exception {
        doPutAllTooLongValueField(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentAsyncTooLongValue() throws Exception {
        doPutIfAbsentTooLongValue(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentAsyncTooLongKey() throws Exception {
        doPutIfAbsentTooLongKey(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentAsyncTooLongKeyField() throws Exception {
        doPutIfAbsentTooLongKeyField(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentAsyncTooLongValueField() throws Exception {
        doPutIfAbsentTooLongValueField(true);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutTooLongValue(final boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<String, String> strStrCache = jcache(0, "STR_STR");

            if (async)
                strStrCache.putAsync("1", "123456").get(FUT_TIMEOUT);
            else
                strStrCache.put("1", "123456");

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutTooLongKey(boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<String, String> strStrCache = jcache(0, "STR_STR");

            if (async)
                strStrCache.putAsync("123456", "1").get(FUT_TIMEOUT);
            else
                strStrCache.put("123456", "1");

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutTooLongKeyField(final boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<Organization, Address> orgAddressCache = jcache(0, "ORG_ADDRESS");
            
            Organization org = new Organization("123456");

            Address addr = new Address("1");

            if (async)
                orgAddressCache.putAsync(org, addr).get(FUT_TIMEOUT);
            else
                orgAddressCache.put(org, addr);

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutTooLongValueField(final boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<Organization, Address> orgAddressCache = jcache(0, "ORG_ADDRESS");

            Organization org = new Organization("1");

            Address addr = new Address("123456");

            if (async)
                orgAddressCache.putAsync(org, addr).get(FUT_TIMEOUT);
            else
                orgAddressCache.put(org, addr);

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutAllTooLongValue(final boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<String, String> strStrCache = jcache(0, "STR_STR");
            
            Map<String, String> entries = new HashMap<>();

            entries.put("1", "1");
            entries.put("2", "123456");

            if (async)
                strStrCache.putAllAsync(entries).get(FUT_TIMEOUT);
            else
                strStrCache.putAll(entries);

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutAllTooLongKey(final boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<String, String> strStrCache = jcache(0, "STR_STR");

            Map<String, String> entries = new HashMap<>();

            entries.put("1", "1");
            entries.put("123456", "2");

            if (async)
                strStrCache.putAllAsync(entries).get(FUT_TIMEOUT);
            else
                strStrCache.putAll(entries);

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutAllTooLongKeyField(final boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<Organization, Address> orgAddressCache = jcache(0, "ORG_ADDRESS");

            Map<Organization, Address> entries = new HashMap<>();

            entries.put(new Organization("1"), new Address("1"));
            entries.put(new Organization("123456"), new Address("2"));

            if (async)
                orgAddressCache.putAllAsync(entries).get(FUT_TIMEOUT);
            else
                orgAddressCache.putAll(entries);

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutAllTooLongValueField(final boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<Organization, Address> orgAddressCache = jcache(0, "ORG_ADDRESS");

            Map<Organization, Address> entries = new HashMap<>();

            entries.put(new Organization("1"), new Address("1"));
            entries.put(new Organization("2"), new Address("123456"));

            if (async)
                orgAddressCache.putAllAsync(entries).get(FUT_TIMEOUT);
            else
                orgAddressCache.putAll(entries);

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutIfAbsentTooLongValue(final boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<String, String> strStrCache = jcache(0, "STR_STR");

            if (async)
                strStrCache.putIfAbsentAsync("1", "123456").get(FUT_TIMEOUT);
            else
                strStrCache.putIfAbsent("1", "123456");

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutIfAbsentTooLongKey(final boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<String, String> strStrCache = jcache(0, "STR_STR");

            if (async)
                strStrCache.putIfAbsentAsync("123456", "1").get(FUT_TIMEOUT);
            else
                strStrCache.putIfAbsent("123456", "1");

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutIfAbsentTooLongKeyField(final boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<Organization, Address> orgAddressCache = jcache(0, "ORG_ADDRESS");

            Organization org = new Organization("123456");

            Address addr = new Address("1");
            
            if (async)
                orgAddressCache.putIfAbsentAsync(org, addr).get(FUT_TIMEOUT);
            else
                orgAddressCache.putIfAbsent(org, addr);

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param async Flag to indicate operation has to be called asynchronously.
     */
    private void doPutIfAbsentTooLongValueField(final boolean async) {
        GridTestUtils.assertThrowsWithCause(() -> {
            IgniteCache<Organization, Address> orgAddressCache = jcache(0, "ORG_ADDRESS");

            Organization org = new Organization("1");

            Address addr = new Address("123456");

            orgAddressCache.putIfAbsent(org, addr);

            return 0;
        }, IgniteException.class);
    }

    /**
     * @param qryEntity Query entity.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(QueryEntity qryEntity) {
        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        cache.setQueryEntities(Collections.singletonList(qryEntity));

        return cache;
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
