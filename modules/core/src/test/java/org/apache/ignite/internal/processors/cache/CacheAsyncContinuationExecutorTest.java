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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * Tests {@link IgniteConfiguration#setAsyncContinuationExecutor(Executor)}
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

    /**
     * Tests future listen with default executor.
     *
     * This test would hang before {@link IgniteConfiguration#setAsyncContinuationExecutor(Executor)}
     * was introduced, or if we set {@link Runnable#run()} as the executor.
     */
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testListenDefaultConfig() throws Exception {
        Optional<String> keyOpt = IntStream.range(0, 1000)
                .mapToObj(String::valueOf)
                .filter(x -> belongs(x, 1))
                .findFirst();

        final String key = keyOpt.get();

        IgniteCache<String, Integer> cache = jcache(0);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<String> asyncThreadName = new AtomicReference<>("");

        cache.putAsync(key, 1).listen(f -> {
            asyncThreadName.set(Thread.currentThread().getName());
            cache.replace(key, 2);

            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        barrier.await(5, TimeUnit.SECONDS);
        assertEquals(2, cache.get(key).intValue());
        assertTrue(asyncThreadName.get(), asyncThreadName.get().startsWith("ForkJoinPool.commonPool-worker"));
    }
}
