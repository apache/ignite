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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.client.Person;
import org.apache.ignite.client.PersonBinarylizable;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Thin client async cache tests.
 */
public class CacheAsyncTest extends AbstractThinClientTest {
    /** Default timeout value. */
    private static final long TIMEOUT = 1_000L;

    /** Temp cache name. */
    private static final String TMP_CACHE_NAME = "tmp_cache";

    /** Client. */
    private static IgniteClient client;

    /** */
    private static ClientCache<Integer, Person> personCache;

    /** */
    private static ClientCache<Integer, String> strCache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
        client = startClient(0);

        personCache = client.getOrCreateCache("personCache");
        strCache = client.getOrCreateCache("intCache");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        client.close();
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        strCache.removeAll();
        personCache.removeAll();

        if (client.cacheNames().contains(TMP_CACHE_NAME))
            client.destroyCache(TMP_CACHE_NAME);
    }

    /**
     * Tests async cache creation.
     */
    @Test
    public void testCreateCacheAsyncByNameCreatesCacheWhenNotExists() throws Exception {
        assertTrue(
                client.createCacheAsync(TMP_CACHE_NAME)
                        .thenApply(x -> client.cacheNames().contains(TMP_CACHE_NAME))
                        .toCompletableFuture()
                        .get());
    }

    /**
     * Tests async cache creation with existing name.
     */
    @Test
    public void testCreateCacheAsyncByNameThrowsExceptionWhenCacheExists() throws Exception {
        client.createCache(TMP_CACHE_NAME);

        Throwable t = client.createCacheAsync(TMP_CACHE_NAME)
                .handle((res, err) -> err)
                .toCompletableFuture()
                .get()
                .getCause();

        assertEquals(ClientException.class, t.getClass());
        assertTrue(t.getMessage(), t.getMessage().contains(
                "Failed to start cache (a cache with the same name is already started): tmp_cache"));
    }

    /**
     * Tests async cache creation.
     */
    @Test
    public void testCreateCacheAsyncByCfgCreatesCacheWhenNotExists() throws Exception {
        ClientCacheConfiguration cfg = new ClientCacheConfiguration()
                .setName(TMP_CACHE_NAME)
                .setBackups(3)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        assertTrue(
                client.createCacheAsync(cfg)
                        .thenApply(x -> client.cacheNames().contains(TMP_CACHE_NAME))
                        .toCompletableFuture()
                        .get());

        ClientCacheConfiguration resCfg = client.cache(TMP_CACHE_NAME).getConfiguration();
        assertEquals(3, resCfg.getBackups());
        assertEquals(CacheAtomicityMode.TRANSACTIONAL, resCfg.getAtomicityMode());
    }

    /**
     * Tests async cache creation with existing name.
     */
    @Test
    public void testCreateCacheAsyncByCfgThrowsExceptionWhenCacheExists() throws Exception {
        client.createCache(TMP_CACHE_NAME);

        ClientCacheConfiguration cfg = new ClientCacheConfiguration()
                .setName(TMP_CACHE_NAME)
                .setBackups(3);

        Throwable t = client.createCacheAsync(cfg)
                .handle((res, err) -> err)
                .toCompletableFuture()
                .get()
                .getCause();

        assertEquals(ClientException.class, t.getClass());
        assertTrue(t.getMessage(), t.getMessage().contains(
                "Failed to start cache (a cache with the same name is already started): tmp_cache"));
    }

    /**
     * Tests async cache creation.
     */
    @Test
    public void testGetOrCreateCacheAsyncByCfgCreatesCacheWhenNotExists() throws Exception {
        ClientCacheConfiguration cfg = new ClientCacheConfiguration()
                .setName(TMP_CACHE_NAME)
                .setBackups(5);

        assertTrue(
                client.getOrCreateCacheAsync(cfg)
                        .thenApply(x -> client.cacheNames().contains(TMP_CACHE_NAME))
                        .toCompletableFuture()
                        .get());

        ClientCacheConfiguration resCfg = client.cache(TMP_CACHE_NAME).getConfiguration();
        assertEquals(5, resCfg.getBackups());
    }

    /**
     * Tests async cache creation with existing name.
     */
    @Test
    public void testGetOrCreateCacheAsyncByCfgIgnoresCfgWhenCacheExists() throws Exception {
        client.createCache(TMP_CACHE_NAME);

        ClientCacheConfiguration cfg = new ClientCacheConfiguration()
                .setName(TMP_CACHE_NAME)
                .setBackups(7);

        ClientCache<Object, Object> cache = client.getOrCreateCacheAsync(cfg).get();
        ClientCacheConfiguration resCfg = cache.getConfigurationAsync().get();

        assertEquals(0, resCfg.getBackups());
    }

    /**
     * Tests async cache creation.
     */
    @Test
    public void testGetOrCreateCacheAsyncByNameCreatesCacheWhenNotExists() throws Exception {
        ClientCacheConfiguration cfg = new ClientCacheConfiguration()
                .setName(TMP_CACHE_NAME)
                .setBackups(5);

        assertTrue(
                client.getOrCreateCacheAsync(cfg)
                        .thenApply(x -> client.cacheNames().contains(TMP_CACHE_NAME))
                        .toCompletableFuture()
                        .get());

        ClientCacheConfiguration resCfg = client.cache(TMP_CACHE_NAME).getConfiguration();
        assertEquals(5, resCfg.getBackups());
    }

    /**
     * Tests async cache creation with existing name.
     */
    @Test
    public void testGetOrCreateCacheAsyncByNameReturnsExistingWhenCacheExists() throws Exception {
        ClientCacheConfiguration cfg = new ClientCacheConfiguration()
                .setName(TMP_CACHE_NAME)
                .setBackups(7);

        client.createCache(cfg);

        ClientCache<Object, Object> cache = client.getOrCreateCacheAsync(TMP_CACHE_NAME).get();
        ClientCacheConfiguration resCfg = cache.getConfigurationAsync().get();

        assertEquals(7, resCfg.getBackups());
    }

    /**
     * Tests async cache destroy.
     */
    @Test
    public void testDestroyCacheAsyncSucceedsWhenCacheExists() throws Exception {
        client.createCache(TMP_CACHE_NAME);
        client.destroyCacheAsync(TMP_CACHE_NAME).get();

        assertFalse(client.cacheNames().contains(TMP_CACHE_NAME));
    }

    /**
     * Tests async cache destroy with non-existing cache name.
     */
    @Test
    public void testDestroyCacheAsyncThrowsWhenCacheDoesNotExist() {
        GridTestUtils.assertThrowsAnyCause(null, () -> client.destroyCacheAsync(TMP_CACHE_NAME).get(),
                ClientException.class, "Cache does not exist [cacheId= 911828570]");
    }

    /**
     * Tests async cacheNames.
     *
     * - Get cache names, verify against predefined caches
     * - Create new cache
     * - Get cache names, verify new cache
     */
    @Test
    public void testCacheNamesAsync() throws Exception {
        Collection<String> names = client.cacheNamesAsync().get();

        assertEquals(2, names.size());
        assertTrue(names.contains(personCache.getName()));
        assertTrue(names.contains(strCache.getName()));

        client.createCache(TMP_CACHE_NAME);

        names = client.cacheNamesAsync().get();

        assertEquals(3, names.size());
        assertTrue(names.contains(TMP_CACHE_NAME));
    }

    /**
     * Tests IgniteClientFuture state transitions with getAsync.
     * <p>
     * - Start an async operation
     * - Check that IgniteFuture is not done initially
     * - Wait for operation completion
     * - Verify that listener callback has been called
     * - Verify that operation result is correct
     */
    @Test
    public void testGetAsyncReportsCorrectIgniteFutureStates() throws Exception {
        Person val = new Person(1, Integer.toString(1));
        personCache.put(1, val);

        IgniteClientFuture<Person> fut = personCache.getAsync(1);
        assertFalse(fut.isDone());

        AtomicReference<String> completionThreadName = new AtomicReference<>();
        fut.thenRun(() -> completionThreadName.set(Thread.currentThread().getName()));

        Person res = fut.get();
        assertEquals("1", res.getName());
        assertTrue(fut.isDone());

        assertTrue(GridTestUtils.waitForCondition(() -> completionThreadName.get() != null, TIMEOUT));
        assertFalse("Async operation should not complete on thin client listener thread",
                completionThreadName.get().startsWith("thin-client-channel"));
    }

    /**
     * Tests that async operation can be cancelled.
     * <p>
     * - Start an async operation
     * - Check that cancel returns true and future becomes cancelled
     */
    @Test
    public void testGetAsyncCanBeCancelled() {
        strCache.put(1, "2");

        IgniteClientFuture<String> fut = strCache.getAsync(1);

        assertTrue(fut.cancel(true));
        assertTrue(fut.isCancelled());
        GridTestUtils.assertThrowsAnyCause(null, fut::get, CancellationException.class, null);
    }

    /**
     * Tests getAsync with non-existing cache.
     * <p>
     * - Get a cache that does not exist
     * - Perform getAsync, verify exception in future
     */
    @Test
    public void testGetAsyncThrowsExceptionOnBadCacheName() throws Exception {
        ClientCache<String, String> badCache = client.cache("bad");
        Throwable err = badCache.putAsync("1", "2").handle((r, e) -> e).toCompletableFuture().get();
        assertTrue(err.getMessage().contains("Cache does not exist"));
    }

    /**
     * Tests that response decode errors are handled correctly.
     */
    @Test
    public void testGetAsyncThrowsExceptionOnFailedDeserialization() throws Exception {
        ClientCache<Integer, PersonBinarylizable> cache = client.createCache(TMP_CACHE_NAME);
        cache.put(1, new PersonBinarylizable("1", false, true, false));

        Throwable t = cache.getAsync(1).handle((res, err) -> err).toCompletableFuture().get();

        assertTrue(t.getMessage().contains("Failed to deserialize object"));
        assertTrue(X.hasCause(t, "Failed to deserialize object", ClientException.class));
        assertTrue(X.hasCause(t, "_read_", ArithmeticException.class));
    }

    /**
     * Tests that request encode errors are handled correctly.
     */
    @Test
    public void testPutAsyncThrowsExceptionOnFailedSerialization() {
        ClientCache<Integer, PersonBinarylizable> cache = client.createCache(TMP_CACHE_NAME);

        IgniteClientFuture<Void> fut = cache.putAsync(1,
            new PersonBinarylizable("1", true, false, false));

        GridTestUtils.assertThrowsAnyCause(null, fut::get, BinaryObjectException.class,
            "Failed to serialize object [typeName=org.apache.ignite.client.PersonBinarylizable]");
    }

    /**
     * Tests normal operation of all async cache APIs.
     */
    @Test
    public void testAsyncCacheOperations() throws Exception {
        // Get.
        strCache.put(1, "1");
        assertTrue(strCache.getAsync(1).thenApply("1"::equals).toCompletableFuture().get());
        assertTrue(strCache.getAsync(2).thenApply(Objects::isNull).toCompletableFuture().get());

        // Put.
        strCache.putAsync(11, "2");
        assertTrue(GridTestUtils.waitForCondition(() -> strCache.get(11) != null, TIMEOUT));
        assertEquals("2", strCache.get(11));

        // ContainsKey.
        assertTrue(strCache.containsKeyAsync(1).get());
        assertFalse(strCache.containsKeyAsync(2).get());

        // GetConfiguration.
        assertEquals(strCache.getName(), strCache.getConfigurationAsync().get().getName());

        // Size.
        strCache.put(2, "2");
        assertEquals(3, strCache.sizeAsync().get().intValue());
        assertEquals(0, strCache.sizeAsync(CachePeekMode.BACKUP).get().intValue());
        assertEquals(3, strCache.sizeAsync(CachePeekMode.PRIMARY).get().intValue());

        // GetAll.
        strCache.put(3, "3");
        Map<Integer, String> getAllRes = strCache.getAllAsync(ImmutableSet.of(2, 3, 4, 5)).get();
        assertEquals(2, getAllRes.size());
        assertEquals("2", getAllRes.get(2));
        assertEquals("3", getAllRes.get(3));

        // PutAll.
        strCache.putAllAsync(ImmutableMap.of(4, "4", 5, "5")).get();
        assertEquals("4", strCache.get(4));
        assertEquals("5", strCache.get(5));

        // ContainsKeys.
        assertTrue(strCache.containsKeysAsync(ImmutableSet.of(4, 5)).get());
        assertFalse(strCache.containsKeysAsync(ImmutableSet.of(4, 5, 6)).get());
        assertTrue(strCache.containsKeysAsync(Collections.emptySet()).get());

        // Replace(k, v).
        assertTrue(strCache.replaceAsync(4, "6").get());
        assertEquals("6", strCache.get(4));

        assertFalse(strCache.replaceAsync(-1, "1").get());

        // Replace(k, v1, v2).
        assertTrue(strCache.replaceAsync(4, "6", "7").get());
        assertEquals("7", strCache.get(4));

        assertFalse(strCache.replaceAsync(-1, "1", "2").get());
        assertFalse(strCache.replaceAsync(4, "1", "2").get());

        // Remove(k).
        assertFalse(strCache.removeAsync(-1).get());
        assertTrue(strCache.removeAsync(4).get());
        assertFalse(strCache.containsKeyAsync(4).get());

        // Remove(k, v).
        assertFalse(strCache.remove(2, "0"));
        assertTrue(strCache.remove(2, "2"));
        assertFalse(strCache.containsKey(2));

        // RemoveAll.
        strCache.removeAllAsync().get();
        assertEquals(0, strCache.size());

        // RemoveAll(k).
        strCache.putAll(ImmutableMap.of(1, "1", 2, "2", 3, "3"));
        strCache.removeAllAsync(ImmutableSet.of(2, 3)).get();
        assertEquals(1, strCache.size());
        assertEquals("1", strCache.get(1));

        // GetAndPut.
        assertNull(strCache.getAndPutAsync(2, "2").get());
        assertEquals("2", strCache.getAndPutAsync(2, "3").get());

        // GetAndRemove.
        assertNull(strCache.getAndRemoveAsync(-1).get());
        assertEquals("3", strCache.getAndRemoveAsync(2).get());

        // GetAndReplace.
        assertNull(strCache.getAndReplaceAsync(-1, "1").get());
        assertEquals("1", strCache.getAndReplaceAsync(1, "2").get());

        // PutIfAbsent.
        assertFalse(strCache.putIfAbsentAsync(1, "3").get());
        assertEquals("2", strCache.get(1));

        assertTrue(strCache.putIfAbsentAsync(5, "5").get());
        assertEquals("5", strCache.get(5));

        // Clear.
        strCache.clearAsync().get();
        assertEquals(0, strCache.size());

        // GetAnfPutIfAbsent.
        strCache.putAll(ImmutableMap.of(1, "1", 2, "2", 3, "3"));
        assertEquals("1", strCache.getAndPutIfAbsentAsync(1, "2").get());
        assertEquals("1", strCache.get(1));
        assertNull(strCache.getAndPutIfAbsentAsync(4, "4").get());
        assertEquals("4", strCache.get(4));

        // Clear(k).
        assertEquals("1", strCache.get(1));
        strCache.clearAsync(1).get();
        assertNull(strCache.get(1));

        // ClearAll(k).
        assertEquals(3, strCache.size());
        strCache.clearAllAsync(ImmutableSet.of(2, 3, 4)).get();
        assertEquals(0, strCache.size());
    }
}
