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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Verifies that containsKey() works as expected on atomic cache.
 */
public class IgniteCacheContainsKeyAtomicTest extends IgniteCacheContainsKeyAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testContainsPutIfAbsent() throws Exception {
        Ignite srv = ignite(0);

        final IgniteCache<Integer, Integer> cache = srv.createCache(replicatedCache());

        final AtomicInteger fails = new AtomicInteger(0);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < 100; i++) {
                    if (!cache.putIfAbsent(i, i)) {
                        if (!cache.containsKey(i))
                            fails.incrementAndGet();
                    }
                }
            }
        }, 100, "put-if-abs");

        assertEquals(0, fails.get());
    }

    /**
     * @return replicated cache configuration.
     */
    private CacheConfiguration<Integer, Integer> replicatedCache() {
        return new CacheConfiguration<Integer, Integer>("replicated")
            .setAtomicityMode(ATOMIC)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setReadFromBackup(false)
            .setCacheMode(REPLICATED);
    }
}
