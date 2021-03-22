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

package org.apache.ignite.internal.processors.cache;

import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

/**
 * Tests {@link IgniteConfiguration#setAsyncContinuationExecutor(Executor)}.
 */
public class CacheAsyncContinuationExecutorTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 0;
    }

    /**
     * Tests future listen with default executor.
     *
     * This test would hang before {@link IgniteConfiguration#setAsyncContinuationExecutor(Executor)}
     * was introduced, or if we set {@link Runnable#run()} as the executor.
     */
    @Test
    public void testListenDefaultConfig() throws Exception {
        final String key = getPrimaryKey(1);

        IgniteCache<String, Integer> cache = jcache(0);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<String> listenThreadName = new AtomicReference<>("");

        cache.putAsync(key, 1).listen(f -> {
            listenThreadName.set(Thread.currentThread().getName());
            cache.replace(key, 2);

            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        barrier.await(5, TimeUnit.SECONDS);
        assertEquals(2, cache.get(key).intValue());
        assertTrue(listenThreadName.get(), listenThreadName.get().startsWith("ForkJoinPool.commonPool-worker"));
    }

    /**
     * Gets the primary key.
     * @param nodeIdx Node index.
     * @return Key.
     */
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private String getPrimaryKey(int nodeIdx) {
        return IntStream.range(0, 1000)
                .mapToObj(String::valueOf)
                .filter(x -> belongs(x, nodeIdx))
                .findFirst()
                .get();
    }
}
