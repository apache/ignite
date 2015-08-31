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

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Grid cache concurrent hash map self test.
 */
public class GridCacheConcurrentMapTest extends GridCommonAbstractTest {
    /** Random. */
    private static final Random RAND = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(LOCAL);
        cc.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cc);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cache(null).removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomEntry() throws Exception {
        IgniteCache<String, String> cache = grid(0).cache(null);

        for (int i = 0; i < 500; i++)
            cache.put("key" + i, "val" + i);

        for (int i = 0; i < 20; i++) {
            Cache.Entry<String, String> entry = cache.randomEntry();

            assert entry != null;

            info("Random entry key: " + entry.getKey());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomEntryMultiThreaded() throws Exception {
        final IgniteCache<String, String> cache = grid(0).cache(null);

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut1 = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    while (!done.get()) {
                        int i = RAND.nextInt(500);

                        boolean rmv = RAND.nextBoolean();

                        if (rmv)
                            cache.remove("key" + i);
                        else
                            cache.put("key" + i, "val" + i);
                    }

                    return null;
                }
            },
            3
        );

        IgniteInternalFuture<?> fut2 = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    while (!done.get()) {
                        Cache.Entry<String, String> entry = cache.randomEntry();

                        info("Random entry key: " + (entry != null ? entry.getKey() : "N/A"));
                    }

                    return null;
                }
            },
            1
        );

        Thread.sleep( 60 * 1000);

        done.set(true);

        fut1.get();
        fut2.get();
    }
}