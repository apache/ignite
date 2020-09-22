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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/** */
public class AtomicConcurrentUnorderedPutAllTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 3;

    /** */
    private static final int THREADS_CNT = 20;

    /** */
    public static final String CACHE_NAME = "test-cache";

    /** */
    private static final int CACHE_SIZE = 1_000;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentPutAll() throws Exception {
        String cacheName = "atomic-cache";

        Ignite ignite = startGridsMultiThreaded(NODES_CNT);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>(cacheName)
                .setAtomicityMode(ATOMIC).setBackups(1));

        CyclicBarrier barrier = new CyclicBarrier(THREADS_CNT);

        AtomicInteger threadCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            int threadIdx = threadCnt.incrementAndGet();

            IgniteCache<Object, Object> cache0 = grid(ThreadLocalRandom.current().nextInt(NODES_CNT)).cache(cacheName);

            Map<Object, Object> map = new LinkedHashMap<>();

            if (threadIdx % 2 == 0) {
                for (int i = 0; i < CACHE_SIZE; i++)
                    map.put(i, i);
            } else {
                for (int i = CACHE_SIZE - 1; i >= 0; i--)
                    map.put(i, i);
            }

            for (int i = 0; i < 20; i++) {
                try {
                    barrier.await();
                } catch (Exception e) {
                    fail(e.getMessage());
                }

                cache0.putAll(map);

                log.info("Thread " + threadIdx + " iteration " + i + " finished");
            }
        }, THREADS_CNT, "put-all-runner");

        for (int i = 0; i < CACHE_SIZE; i++)
            assertEquals(i, cache.get(i));
    }
}
