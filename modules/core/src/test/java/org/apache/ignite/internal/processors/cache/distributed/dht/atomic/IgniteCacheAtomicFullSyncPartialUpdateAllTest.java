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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;

/** */
public class IgniteCacheAtomicFullSyncPartialUpdateAllTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setUserAttributes(singletonMap(IDX_ATTR, getTestIgniteInstanceIndex(igniteInstanceName)));
    }

    /** */
    @Test
    public void test() throws Exception {
        startGrid(0);
        startGrid(1);
        startGrid(2);

        TestInterceptor.putStartedLatch = new CountDownLatch(1);
        TestInterceptor.putUnblockedLatch = new CountDownLatch(1);

        IgniteCache<Integer, Integer> cache = grid(0).createCache(new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setBackups(2)
            .setAffinity(new GridCacheModuloAffinityFunction(3, 2))
            .setInterceptor(new TestInterceptor())
        );

        Map<Integer, Integer> data = new TreeMap<>();

        data.put(0, 0); // node 0 entry
        data.put(1, 1); // node 1 entry
        data.put(4, 4); // node 1 entry

        IgniteInternalFuture<Object> putFut = GridTestUtils.runAsync(() -> cache.putAll(data));

        assertTrue(TestInterceptor.putStartedLatch.await(getTestTimeout(), MILLISECONDS));

        IgniteEx stoppingNode = grid(1);

        IgniteInternalFuture<Object> stopFut = GridTestUtils.runAsync(() -> stopGrid(1));

        try {
            GridTestUtils.waitForCondition(() ->
                U.<AtomicBoolean>field(stoppingNode.context().cache().cacheGroup(cacheId(DEFAULT_CACHE_NAME)).offheap(), "stopping").get(),
                getTestTimeout()
            );

            TestInterceptor.putUnblockedLatch.countDown();

            putFut.get();

            assertNotNull(cache.get(0));
            assertNotNull(cache.get(1));
            assertNotNull(cache.get(4));
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.getMessage().contains("Failed to update keys (retry update if possible)"));
        }
        finally {
            stopFut.get();
        }
    }

    /** */
    public static final class TestInterceptor implements CacheInterceptor<Integer, Integer> {
        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** */
        public static CountDownLatch putStartedLatch;

        /** */
        public static CountDownLatch putUnblockedLatch;

        /** {@inheritDoc} */
        @Override public @Nullable Integer onGet(Integer key, @Nullable Integer val) {
            return val;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer onBeforePut(Cache.Entry<Integer, Integer> entry, Integer newVal) {
            // Node with index 1 is primary for key 1
            if (newVal == 1 && ignite.localNode().<Integer>attribute(IDX_ATTR) == 1) {
                putStartedLatch.countDown();

                try {
                    assertTrue(putUnblockedLatch.await(5000, MILLISECONDS));
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteException(e);
                }
            }

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<Integer, Integer> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public @Nullable IgniteBiTuple<Boolean, Integer> onBeforeRemove(Cache.Entry<Integer, Integer> entry) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<Integer, Integer> entry) {
            // No-op.
        }
    }
}
