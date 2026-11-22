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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Checks the near cache of a client that sends an ATOMIC invokeAll of several keys to a cache with a write-through
 * store. The primary node processes such a request in {@code GridDhtAtomicCache.updateWithBatch()}. Each test leaves
 * the key in the middle of the request unwritten, so the near cache must keep its old value for this key.
 */
public class AtomicNearCacheInvokeAllWithStoreTest extends GridCommonAbstractTest {
    /** Value of the keys before the invokeAll. */
    private static final Integer OLD_VAL = -1;

    /** Keys of the invokeAll, sorted. The only server node is the primary node of every key. */
    private static final List<Integer> KEYS = Arrays.asList(1, 2, 3);

    /** Key in the middle of the request. */
    private static final Integer MIDDLE_KEY = KEYS.get(1);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** The entry processor leaves the middle key unchanged. */
    @Test
    public void testUnchangedKeyInTheMiddleOfInvokeAll() throws Exception {
        checkMiddleKeyKeepsOldValue(cacheConfiguration(), new SetKeyAsValue(MIDDLE_KEY));
    }

    /**
     * The entry processor leaves the middle key unchanged. The cache has an expiry policy, so the response to the
     * client also has a TTL for each key.
     */
    @Test
    public void testUnchangedKeyInTheMiddleOfInvokeAllWithExpiryPolicy() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration()
            .setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 10)));

        checkMiddleKeyKeepsOldValue(ccfg, new SetKeyAsValue(MIDDLE_KEY));
    }

    /** The entry processor changes every key, and the interceptor cancels the update of the middle key. */
    @Test
    public void testInterceptorCancelsKeyInTheMiddleOfInvokeAll() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration().setInterceptor(new CancelMiddleKeyUpdate());

        checkMiddleKeyKeepsOldValue(ccfg, new SetKeyAsValue(null));
    }

    /**
     * @return Configuration of an ATOMIC cache with a write-through store.
     */
    private static CacheConfiguration<Integer, Integer> cacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setCacheStoreFactory(FactoryBuilder.factoryOf(NoopStore.class))
            .setWriteThrough(true);
    }

    /**
     * Starts a server node and a client node with a near cache, puts the old value to every key, and runs the
     * invokeAll from the client. Then the near cache must have the old value for the middle key, and the key as the
     * value for every other key.
     *
     * @param ccfg Cache configuration.
     * @param entryProc Entry processor of the invokeAll.
     * @throws Exception If failed.
     */
    private void checkMiddleKeyKeepsOldValue(
        CacheConfiguration<Integer, Integer> ccfg,
        CacheEntryProcessor<Integer, Integer, Void> entryProc
    ) throws Exception {
        IgniteEx srv = startGrid(0);
        IgniteEx client = startClientGrid(1);

        srv.createCache(ccfg);

        IgniteCache<Integer, Integer> cache =
            client.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>());

        // Puts the keys one by one, so the near cache of the client has all of them.
        for (Integer key : KEYS)
            cache.put(key, OLD_VAL);

        Throwable err = null;

        try {
            // Sorted, so the middle key is in the middle of the request.
            cache.invokeAllAsync(new TreeSet<>(KEYS), entryProc).get(10_000);
        }
        catch (Exception e) {
            err = e;
        }

        assertEquals(KEYS.get(0), cache.localPeek(KEYS.get(0), CachePeekMode.NEAR));
        assertEquals("The middle key has a wrong value in the near cache",
            OLD_VAL, cache.localPeek(MIDDLE_KEY, CachePeekMode.NEAR));
        assertEquals(KEYS.get(2), cache.localPeek(KEYS.get(2), CachePeekMode.NEAR));

        assertNull("invokeAll failed: " + err, err);
    }

    /** Sets the key as the value of every key except one, which it leaves unchanged. */
    private static class SetKeyAsValue implements CacheEntryProcessor<Integer, Integer, Void> {
        /** Key to leave unchanged, or {@code null} to change every key. */
        private final Integer unchangedKey;

        /** */
        SetKeyAsValue(Integer unchangedKey) {
            this.unchangedKey = unchangedKey;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> entry, Object... args) {
            if (!entry.getKey().equals(unchangedKey))
                entry.setValue(entry.getKey());

            return null;
        }
    }

    /** Cancels every update of the middle key, except the put of the old value. */
    private static class CancelMiddleKeyUpdate extends CacheInterceptorAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer onBeforePut(Cache.Entry<Integer, Integer> entry, Integer newVal) {
            return MIDDLE_KEY.equals(entry.getKey()) && !OLD_VAL.equals(newVal) ? null : newVal;
        }
    }

    /** Store that keeps nothing, so the cache can use write-through. */
    public static class NoopStore extends CacheStoreAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer load(Integer key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }
}
