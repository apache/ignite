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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Checks the near cache of a client that sends an ATOMIC request of several keys to a cache with a write-through
 * store. The primary node processes such a request in {@code GridDhtAtomicCache.updateWithBatch()}. Each test leaves
 * one key of the request unwritten, so the near cache must keep its old value for this key.
 */
public class AtomicNearCacheInvokeAllWithStoreTest extends GridCommonAbstractTest {
    /** Value of the keys before the request. */
    private static final Integer OLD_VAL = -1;

    /** Keys of the request, sorted. The only server node is the primary node of every key. */
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
        checkNearValues(cacheConfiguration(), KEYS, invokeAll(KEYS, new UpdateByKey(null, MIDDLE_KEY)),
            Arrays.asList(1, OLD_VAL, 3));
    }

    /**
     * The entry processor leaves the middle key unchanged. The cache has an expiry policy, so the response to the
     * client also has a TTL for each key.
     */
    @Test
    public void testUnchangedKeyInTheMiddleOfInvokeAllWithExpiryPolicy() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration()
            .setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 10)));

        checkNearValues(ccfg, KEYS, invokeAll(KEYS, new UpdateByKey(null, MIDDLE_KEY)),
            Arrays.asList(1, OLD_VAL, 3));
    }

    /** The entry processor changes every key, and the interceptor cancels the update of the middle key. */
    @Test
    public void testInterceptorCancelsKeyInTheMiddleOfInvokeAll() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration().setInterceptor(new CancelMiddleKeyUpdate());

        checkNearValues(ccfg, KEYS, invokeAll(KEYS, new UpdateByKey(null, null)), Arrays.asList(1, OLD_VAL, 3));
    }

    /** The entry processor removes every key, and the interceptor cancels the remove of the middle key. */
    @Test
    public void testInterceptorCancelsKeyInTheMiddleOfInvokeAllRemove() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration().setInterceptor(new CancelMiddleKeyUpdate());

        checkNearValues(ccfg, KEYS, invokeAll(KEYS, new UpdateByKey(KEYS, null)), Arrays.asList(null, OLD_VAL, null));
    }

    /** The interceptor cancels the put of the middle key in a putAll. */
    @Test
    public void testInterceptorCancelsKeyInTheMiddleOfPutAll() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration().setInterceptor(new CancelMiddleKeyUpdate());

        Map<Integer, Integer> vals = new TreeMap<>();

        for (Integer key : KEYS)
            vals.put(key, key);

        checkNearValues(ccfg, KEYS, c -> c.putAllAsync(vals), Arrays.asList(1, OLD_VAL, 3));
    }

    /** The interceptor cancels the remove of the middle key in a removeAll. */
    @Test
    public void testInterceptorCancelsKeyInTheMiddleOfRemoveAll() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration().setInterceptor(new CancelMiddleKeyUpdate());

        checkNearValues(ccfg, KEYS, c -> c.removeAllAsync(new TreeSet<>(KEYS)), Arrays.asList(null, OLD_VAL, null));
    }

    /**
     * The entry processor sets, removes and sets the first keys, so the primary node writes them in three batches.
     * Then it leaves the next key unchanged and sets the last key.
     */
    @Test
    public void testUnchangedKeyAfterPutAndRemoveBatchesOfInvokeAll() throws Exception {
        List<Integer> keys = Arrays.asList(1, 2, 3, 4, 5);

        checkNearValues(cacheConfiguration(), keys, invokeAll(keys, new UpdateByKey(Collections.singletonList(2), 4)),
            Arrays.asList(1, null, 3, OLD_VAL, 5));
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
     * @param keys Keys.
     * @param entryProc Entry processor.
     * @return Operation that runs an invokeAll of the keys, sorted.
     */
    private static IgniteClosure<IgniteCache<Integer, Integer>, IgniteFuture<?>> invokeAll(
        Collection<Integer> keys,
        CacheEntryProcessor<Integer, Integer, Void> entryProc
    ) {
        return c -> c.invokeAllAsync(new TreeSet<>(keys), entryProc);
    }

    /**
     * Starts a server node and a client node with a near cache, puts the old value to every key, and runs the
     * operation from the client. Then the near cache must have the expected value of every key.
     *
     * @param ccfg Cache configuration.
     * @param keys Keys of the operation, sorted.
     * @param op Operation.
     * @param expVals Expected near cache value of each key, {@code null} for a removed key.
     * @throws Exception If failed.
     */
    private void checkNearValues(
        CacheConfiguration<Integer, Integer> ccfg,
        List<Integer> keys,
        IgniteClosure<IgniteCache<Integer, Integer>, IgniteFuture<?>> op,
        List<Integer> expVals
    ) throws Exception {
        IgniteEx srv = startGrid(0);
        IgniteEx client = startClientGrid(1);

        srv.createCache(ccfg);

        IgniteCache<Integer, Integer> cache =
            client.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>());

        // Puts the keys one by one, so the near cache of the client has all of them.
        for (Integer key : keys)
            cache.put(key, OLD_VAL);

        Throwable err = null;

        try {
            op.apply(cache).get(10_000);
        }
        catch (Exception e) {
            err = e;
        }

        for (int i = 0; i < keys.size(); i++) {
            assertEquals("Key " + keys.get(i) + " has a wrong value in the near cache",
                expVals.get(i), cache.localPeek(keys.get(i), CachePeekMode.NEAR));
        }

        assertNull("The operation failed: " + err, err);
    }

    /** Removes the keys to remove, leaves one key unchanged, and sets the key as the value of every other key. */
    private static class UpdateByKey implements CacheEntryProcessor<Integer, Integer, Void> {
        /** Keys to remove, or {@code null} to remove none. */
        private final Collection<Integer> rmvKeys;

        /** Key to leave unchanged, or {@code null} to change every key. */
        private final Integer unchangedKey;

        /**
         * @param rmvKeys Keys to remove, or {@code null} to remove none.
         * @param unchangedKey Key to leave unchanged, or {@code null} to change every key.
         */
        UpdateByKey(Collection<Integer> rmvKeys, Integer unchangedKey) {
            this.rmvKeys = rmvKeys;
            this.unchangedKey = unchangedKey;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> entry, Object... args) {
            if (rmvKeys != null && rmvKeys.contains(entry.getKey()))
                entry.remove();
            else if (!entry.getKey().equals(unchangedKey))
                entry.setValue(entry.getKey());

            return null;
        }
    }

    /** Cancels every put and remove of the middle key, except the put of the old value. */
    private static class CancelMiddleKeyUpdate extends CacheInterceptorAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer onBeforePut(Cache.Entry<Integer, Integer> entry, Integer newVal) {
            return MIDDLE_KEY.equals(entry.getKey()) && !OLD_VAL.equals(newVal) ? null : newVal;
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<Boolean, Integer> onBeforeRemove(Cache.Entry<Integer, Integer> entry) {
            return new IgniteBiTuple<>(MIDDLE_KEY.equals(entry.getKey()), entry.getValue());
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
