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

import java.util.List;
import java.util.TreeSet;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
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
 * store. The primary node processes such a request in {@code GridDhtAtomicCache.updateWithBatch()}.
 */
public class AtomicNearCacheInvokeAllWithStoreTest extends GridCommonAbstractTest {
    /** Value of the keys before the invokeAll. */
    private static final Integer OLD_VAL = -1;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** The entry processor leaves the middle key unchanged, so the near cache must keep its old value. */
    @Test
    public void testUnchangedKeyInTheMiddleOfInvokeAll() throws Exception {
        IgniteEx srv = startGrid(0);
        IgniteEx client = startClientGrid(1);

        srv.createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setCacheStoreFactory(FactoryBuilder.factoryOf(NoopStore.class))
            .setWriteThrough(true));

        IgniteCache<Integer, Integer> cache =
            client.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>());

        List<Integer> keys = primaryKeys(srv.cache(DEFAULT_CACHE_NAME), 3);

        // Puts the keys one by one, so the near cache of the client has all of them.
        for (Integer key : keys)
            cache.put(key, OLD_VAL);

        Throwable err = null;

        try {
            // Sorted, so the unchanged key is in the middle of the request.
            cache.invokeAllAsync(new TreeSet<>(keys), new SetKeyAsValue(keys.get(1))).get(10_000);
        }
        catch (Exception e) {
            err = e;
        }

        assertEquals(keys.get(0), cache.localPeek(keys.get(0), CachePeekMode.NEAR));
        assertEquals("The unchanged key has a wrong value in the near cache",
            OLD_VAL, cache.localPeek(keys.get(1), CachePeekMode.NEAR));
        assertEquals(keys.get(2), cache.localPeek(keys.get(2), CachePeekMode.NEAR));

        assertNull("invokeAll failed: " + err, err);
    }

    /** Sets the key as the value of every key except one, which it leaves unchanged. */
    private static class SetKeyAsValue implements CacheEntryProcessor<Integer, Integer, Void> {
        /** Key to leave unchanged. */
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
