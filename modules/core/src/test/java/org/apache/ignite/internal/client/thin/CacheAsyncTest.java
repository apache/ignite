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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.client.Person;
import org.apache.ignite.client.PersonBinarylizable;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thin client async cache tests.
 */
public class CacheAsyncTest extends AbstractThinClientTest {
    // TODO: getOrCreateAsync, destroyAsync, cacheNamesAsync

    /**
     * Default timeout value.
     */
    private static final long TIMEOUT = 1_000L;

    /**
     * Temp cache name.
     */
    private static final String TMP_CACHE_NAME = "tmp_cache";

    /**
     * Client.
     */
    private static IgniteClient client;

    /**
     *
     */
    private static ClientCache<Integer, Person> personCache;

    /**
     *
     */
    private static ClientCache<Integer, String> strCache;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
        client = startClient(0);

        personCache = client.getOrCreateCache("personCache");
        strCache = client.getOrCreateCache("intCache");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        client.close();
        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTest() throws Exception {
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
     * Tests async cache destroy.
     */
    @Test
    public void testDestroyCacheAsyncSucceedsWhenCacheExists() throws Exception {
        client.createCache(TMP_CACHE_NAME);
        client.destroyCacheAsync(TMP_CACHE_NAME).get();

        assertFalse(client.cacheNames().contains(TMP_CACHE_NAME));
    }

    /**
     * Tests async cache destroy.
     */
    @Test
    public void testDestroyCacheAsyncThrowsWhenCacheDoesNotExist() throws Exception {
        GridTestUtils.assertThrowsAnyCause(null, () -> client.destroyCacheAsync(TMP_CACHE_NAME).get(),
                ClientException.class, "Cache does not exist [cacheId= 911828570]");
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
     * Tests getAsync with existing and non-existing keys.
     */
    @Test
    public void testGetAsyncReturnsResult() throws Exception {
        strCache.put(1, "1");
        assertTrue(strCache.getAsync(1).thenApply("1"::equals).toCompletableFuture().get());
        assertTrue(strCache.getAsync(2).thenApply(Objects::isNull).toCompletableFuture().get());
    }

    /**
     * Tests getAsync argument validation.
     */
    @Test
    public void testGetAsyncValidatesArguments() {
        GridTestUtils.assertThrowsAnyCause(
                null, () -> strCache.putAsync(null, "1"), NullPointerException.class, "key");

        GridTestUtils.assertThrowsAnyCause(
                null, () -> strCache.putAsync(1, null), NullPointerException.class, "val");
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
        cache.put(1, new PersonBinarylizable("1", false, true));

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

        GridTestUtils.assertThrowsAnyCause(
            null,
            () -> cache.putAsync(1, new PersonBinarylizable("1", true, false)),
            BinaryObjectException.class,
            "Failed to serialize object [typeName=org.apache.ignite.client.PersonBinarylizable]");
    }

    /**
     * Tests putAsync happy path.
     */
    @Test
    public void testPutAsyncUpdatesCache() throws Exception {
        strCache.putAsync(1, "2");
        assertTrue(GridTestUtils.waitForCondition(() -> strCache.get(1) != null, TIMEOUT));
        assertEquals("2", strCache.get(1));
    }
}
