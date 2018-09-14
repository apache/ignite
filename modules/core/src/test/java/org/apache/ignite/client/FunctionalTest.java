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

package org.apache.ignite.client;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Thin client functional tests.
 */
public class FunctionalTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT);

    /**
     * Tested API:
     * <ul>
     * <li>{@link IgniteClient#cache(String)}</li>
     * <li>{@link IgniteClient#getOrCreateCache(ClientCacheConfiguration)}</li>
     * <li>{@link IgniteClient#cacheNames()}</li>
     * <li>{@link IgniteClient#createCache(String)}</li>
     * <li>{@link IgniteClient#createCache(ClientCacheConfiguration)}</li>
     * <li>{@link IgniteCache#size(CachePeekMode...)}</li>
     * </ul>
     */
    @Test
    public void testCacheManagement() throws Exception {
        try (LocalIgniteCluster ignored = LocalIgniteCluster.start(2);
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            final String CACHE_NAME = "testCacheManagement";

            ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration().setName(CACHE_NAME)
                .setCacheMode(CacheMode.REPLICATED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            int key = 1;
            Person val = new Person(key, Integer.toString(key));

            ClientCache<Integer, Person> cache = client.getOrCreateCache(cacheCfg);

            cache.put(key, val);

            assertEquals(1, cache.size());
            assertEquals(2, cache.size(CachePeekMode.ALL));

            cache = client.cache(CACHE_NAME);

            Person cachedVal = cache.get(key);

            assertEquals(val, cachedVal);

            Object[] cacheNames = new TreeSet<>(client.cacheNames()).toArray();

            assertArrayEquals(new TreeSet<>(Arrays.asList(Config.DEFAULT_CACHE_NAME, CACHE_NAME)).toArray(), cacheNames);

            client.destroyCache(CACHE_NAME);

            cacheNames = client.cacheNames().toArray();

            assertArrayEquals(new Object[] {Config.DEFAULT_CACHE_NAME}, cacheNames);

            cache = client.createCache(CACHE_NAME);

            assertFalse(cache.containsKey(key));

            cacheNames = client.cacheNames().toArray();

            assertArrayEquals(new TreeSet<>(Arrays.asList(Config.DEFAULT_CACHE_NAME, CACHE_NAME)).toArray(), cacheNames);

            client.destroyCache(CACHE_NAME);

            cache = client.createCache(cacheCfg);

            assertFalse(cache.containsKey(key));

            assertArrayEquals(new TreeSet<>(Arrays.asList(Config.DEFAULT_CACHE_NAME, CACHE_NAME)).toArray(), cacheNames);
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link ClientCache#getName()}</li>
     * <li>{@link ClientCache#getConfiguration()}</li>
     * </ul>
     */
    @Test
    public void testCacheConfiguration() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            final String CACHE_NAME = "testCacheConfiguration";

            ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration().setName(CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(3)
                .setCacheMode(CacheMode.PARTITIONED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setEagerTtl(false)
                .setGroupName("FunctionalTest")
                .setDefaultLockTimeout(12345)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_ALL)
                .setReadFromBackup(true)
                .setRebalanceBatchSize(67890)
                .setRebalanceBatchesPrefetchCount(102938)
                .setRebalanceDelay(54321)
                .setRebalanceMode(CacheRebalanceMode.SYNC)
                .setRebalanceOrder(2)
                .setRebalanceThrottle(564738)
                .setRebalanceTimeout(142536)
                .setKeyConfiguration(new CacheKeyConfiguration("Employee", "orgId"))
                .setQueryEntities(new QueryEntity(int.class.getName(), "Employee")
                    .setTableName("EMPLOYEE")
                    .setFields(
                        Stream.of(
                            new SimpleEntry<>("id", Integer.class.getName()),
                            new SimpleEntry<>("orgId", Integer.class.getName())
                        ).collect(Collectors.toMap(
                            SimpleEntry::getKey, SimpleEntry::getValue, (a, b) -> a, LinkedHashMap::new
                        ))
                    )
                    .setKeyFields(Collections.singleton("id"))
                    .setNotNullFields(Collections.singleton("id"))
                    .setDefaultFieldValues(Collections.singletonMap("id", 0))
                    .setIndexes(Collections.singletonList(new QueryIndex("id", true, "IDX_EMPLOYEE_ID")))
                    .setAliases(Stream.of("id", "orgId").collect(Collectors.toMap(f -> f, String::toUpperCase)))
                );

            ClientCache cache = client.createCache(cacheCfg);

            assertEquals(CACHE_NAME, cache.getName());

            assertTrue(Comparers.equal(cacheCfg, cache.getConfiguration()));
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link Ignition#startClient(ClientConfiguration)}</li>
     * <li>{@link IgniteClient#getOrCreateCache(String)}</li>
     * <li>{@link ClientCache#put(Object, Object)}</li>
     * <li>{@link ClientCache#get(Object)}</li>
     * <li>{@link ClientCache#containsKey(Object)}</li>
     * </ul>
     */
    @Test
    public void testPutGet() throws Exception {
        // Existing cache, primitive key and object value
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            ClientCache<Integer, Person> cache = client.getOrCreateCache(Config.DEFAULT_CACHE_NAME);

            Integer key = 1;
            Person val = new Person(key, "Joe");

            cache.put(key, val);

            assertTrue(cache.containsKey(key));

            Person cachedVal = cache.get(key);

            assertEquals(val, cachedVal);
        }

        // Non-existing cache, object key and primitive value
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            ClientCache<Person, Integer> cache = client.getOrCreateCache("testPutGet");

            Integer val = 1;

            Person key = new Person(val, "Joe");

            cache.put(key, val);

            Integer cachedVal = cache.get(key);

            assertEquals(val, cachedVal);
        }

        // Object key and Object value
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            ClientCache<Person, Person> cache = client.getOrCreateCache("testPutGet");

            Person key = new Person(1, "Joe Key");

            Person val = new Person(1, "Joe Value");

            cache.put(key, val);

            Person cachedVal = cache.get(key);

            assertEquals(val, cachedVal);
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link ClientCache#putAll(Map)}</li>
     * <li>{@link ClientCache#getAll(Set)}</li>
     * <li>{@link ClientCache#clear()}</li>
     * </ul>
     */
    @Test
    public void testBatchPutGet() throws Exception {
        // Existing cache, primitive key and object value
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            ClientCache<Integer, Person> cache = client.cache(Config.DEFAULT_CACHE_NAME);

            Map<Integer, Person> data = IntStream
                .rangeClosed(1, 1000).boxed()
                .collect(Collectors.toMap(i -> i, i -> new Person(i, String.format("Person %s", i))));

            cache.putAll(data);

            Map<Integer, Person> cachedData = cache.getAll(data.keySet());

            assertEquals(data, cachedData);
        }

        // Non-existing cache, object key and primitive value
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            ClientCache<Person, Integer> cache = client.createCache("testBatchPutGet");

            Map<Person, Integer> data = IntStream
                .rangeClosed(1, 1000).boxed()
                .collect(Collectors.toMap(i -> new Person(i, String.format("Person %s", i)), i -> i));

            cache.putAll(data);

            Map<Person, Integer> cachedData = cache.getAll(data.keySet());

            assertEquals(data, cachedData);

            cache.clear();

            assertEquals(0, cache.size(CachePeekMode.ALL));
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link ClientCache#getAndPut(Object, Object)}</li>
     * <li>{@link ClientCache#getAndRemove(Object)}</li>
     * <li>{@link ClientCache#getAndReplace(Object, Object)}</li>
     * <li>{@link ClientCache#putIfAbsent(Object, Object)}</li>
     * </ul>
     */
    @Test
    public void testAtomicPutGet() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            ClientCache<Integer, String> cache = client.createCache("testRemoveReplace");

            assertNull(cache.getAndPut(1, "1"));
            assertEquals("1", cache.getAndPut(1, "1.1"));

            assertEquals("1.1", cache.getAndRemove(1));
            assertNull(cache.getAndRemove(1));

            assertTrue(cache.putIfAbsent(1, "1"));
            assertFalse(cache.putIfAbsent(1, "1.1"));

            assertEquals("1", cache.getAndReplace(1, "1.1"));
            assertEquals("1.1", cache.getAndReplace(1, "1"));
            assertNull(cache.getAndReplace(2, "2"));
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link ClientCache#replace(Object, Object)}</li>
     * <li>{@link ClientCache#replace(Object, Object, Object)}</li>
     * <li>{@link ClientCache#remove(Object)}</li>
     * <li>{@link ClientCache#remove(Object, Object)}</li>
     * <li>{@link ClientCache#removeAll()}</li>
     * <li>{@link ClientCache#removeAll(Set)}</li>
     * </ul>
     */
    @Test
    public void testRemoveReplace() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            ClientCache<Integer, String> cache = client.createCache("testRemoveReplace");

            Map<Integer, String> data = IntStream.rangeClosed(1, 100).boxed()
                .collect(Collectors.toMap(i -> i, Object::toString));

            cache.putAll(data);

            assertFalse(cache.replace(1, "2", "3"));
            assertEquals("1", cache.get(1));
            assertTrue(cache.replace(1, "1", "3"));
            assertEquals("3", cache.get(1));

            assertFalse(cache.replace(101, "101"));
            assertNull(cache.get(101));
            assertTrue(cache.replace(100, "101"));
            assertEquals("101", cache.get(100));

            assertFalse(cache.remove(101));
            assertTrue(cache.remove(100));
            assertNull(cache.get(100));

            assertFalse(cache.remove(99, "100"));
            assertEquals("99", cache.get(99));
            assertTrue(cache.remove(99, "99"));
            assertNull(cache.get(99));

            cache.put(101, "101");

            cache.removeAll(data.keySet());
            assertEquals(1, cache.size());
            assertEquals("101", cache.get(101));

            cache.removeAll();
            assertEquals(0, cache.size());
        }
    }

    /**
     * Test client fails on start if server is unavailable
     */
    @Test
    public void testClientFailsOnStart() {
        ClientConnectionException expEx = null;

        try (IgniteClient ignored = Ignition.startClient(getClientConfiguration())) {
            // No-op.
        }
        catch (ClientConnectionException connEx) {
            expEx = connEx;
        }
        catch (Exception ex) {
            fail(String.format(
                "%s expected but %s was received: %s",
                ClientConnectionException.class.getName(),
                ex.getClass().getName(),
                ex
            ));
        }

        assertNotNull(
            String.format("%s expected but no exception was received", ClientConnectionException.class.getName()),
            expEx
        );
    }

    /** */
    private static ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setSendBufferSize(0)
            .setReceiveBufferSize(0);
    }
}
