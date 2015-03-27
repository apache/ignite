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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Tests cache flags.
 */
public abstract class GridCacheAbstractFlagsTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 6;
    }

    /** {@inheritDoc} */
    @Override protected CacheWriteSynchronizationMode writeSynchronization() {
        return FULL_ASYNC;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration c = super.cacheConfiguration(gridName);

        if (cacheMode() == CacheMode.PARTITIONED)
            c.setBackups(1);

        c.setCacheStoreFactory(null);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected boolean swapEnabled() {
        return false;
    }

    /**
     * Tests SYNC_COMMIT cache flag.
     *
     * @throws Exception If failed.
     */
    public void testTestSyncCommitFlag() throws Exception {
        for (int i = 0; i < 10; i++) {
            final String key = "k" + i;
            final Integer val = i;

            final CountDownLatch l = new CountDownLatch(1);

            final AtomicInteger cntr = new AtomicInteger();

            IgniteInternalFuture<?> f = multithreadedAsync(new Callable() {
                @Override public Object call() throws Exception {
                    int idx = cntr.getAndIncrement() % gridCount();

                    IgniteCache<String, Integer> c = jcache(idx);

                    l.await();

                    assertEquals(val, c.get(key));

                    return null;
                }
            }, gridCount() * 3);

            ((IgniteCacheProxy)jcache(0)).flagOn(CacheFlag.SYNC_COMMIT).put(key, val);

            l.countDown();

            f.get();
        }
    }
}
