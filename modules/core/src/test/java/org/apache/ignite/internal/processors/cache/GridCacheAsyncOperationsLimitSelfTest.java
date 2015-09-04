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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.GridAtomicInteger;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Checks that number of concurrent asynchronous operations is limited when configuration parameter is set.
 */
public class GridCacheAsyncOperationsLimitSelfTest extends GridCacheAbstractSelfTest {
    /** */
    public static final int MAX_CONCURRENT_ASYNC_OPS = 50;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cCfg = super.cacheConfiguration(gridName);

        cCfg.setMaxConcurrentAsyncOperations(MAX_CONCURRENT_ASYNC_OPS);

        return cCfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsyncOps() throws Exception {
        final AtomicInteger cnt = new AtomicInteger();
        final GridAtomicInteger max = new GridAtomicInteger();

        for (int i = 0; i < 5000; i++) {
            final int i0 = i;

            cnt.incrementAndGet();

            IgniteCache<String, Integer> cacheAsync = jcache().withAsync();

            cacheAsync.put("key" + i, i);

            IgniteFuture<?> fut = cacheAsync.future();

            fut.listen(new CI1<IgniteFuture<?>>() {
                @Override public void apply(IgniteFuture<?> t) {
                    cnt.decrementAndGet();

                    max.setIfGreater(cnt.get());

                    if (i0 > 0 && i0 % 100 == 0)
                        info("cnt: " + cnt.get());
                }
            });

            assertTrue("Maximum number of permits exceeded: " + max.get(),  max.get() <= 51);
        }
    }
}