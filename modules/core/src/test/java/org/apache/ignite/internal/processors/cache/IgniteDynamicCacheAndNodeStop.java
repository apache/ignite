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
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteDynamicCacheAndNodeStop extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheAndNodeStop() throws Exception {
        final Ignite ignite = startGrid(0);

        for (int i = 0; i < 3; i++) {
            log.info("Iteration: " + i);

            startGrid(1);

            final CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ignite.createCache(ccfg);

            final CyclicBarrier barrier = new CyclicBarrier(2);

            IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    barrier.await();

                    ignite.destroyCache(DEFAULT_CACHE_NAME);

                    return null;
                }
            });

            IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    barrier.await();

                    stopGrid(1);

                    return null;
                }
            });

            fut1.get();
            fut2.get();
        }
    }
}
