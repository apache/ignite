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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.apache.ignite.testframework.MvccFeatureChecker;

/**
 * Test remove all method.
 */
@RunWith(JUnit4.class)
public class CacheRemoveAllSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        if (MvccFeatureChecker.forcedMvcc())
            fail("https://issues.apache.org/jira/browse/IGNITE-10082");

        super.setUp();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAll() throws Exception {
        IgniteCache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10_000; ++i)
            cache.put(i, "val");

        final AtomicInteger igniteId = new AtomicInteger(gridCount());

        IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < 2; ++i)
                    startGrid(igniteId.getAndIncrement());

                return true;
            }
        }, 3, "start-node-thread");

        cache.removeAll();

        fut.get();

        U.sleep(5000);

        for (int i = 0; i < igniteId.get(); ++i) {
            IgniteCache locCache = grid(i).cache(DEFAULT_CACHE_NAME);

            assertEquals("Local size: " + locCache.localSize() + "\n" +
                "On heap: " + locCache.localSize(CachePeekMode.ONHEAP) + "\n" +
                "Off heap: " + locCache.localSize(CachePeekMode.OFFHEAP) + "\n" +
                "Primary: " + locCache.localSize(CachePeekMode.PRIMARY) + "\n" +
                "Backup: " + locCache.localSize(CachePeekMode.BACKUP),
                0, locCache.localSize());
        }
    }
}
