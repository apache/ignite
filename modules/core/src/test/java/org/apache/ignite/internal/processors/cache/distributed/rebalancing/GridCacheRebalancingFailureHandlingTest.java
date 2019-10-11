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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;

/**
 * Test case for checking uncaught exceptions handling during rebalancing.
 */
public class GridCacheRebalancingFailureHandlingTest extends GridCommonAbstractTest {
    /** Node failure occurs. */
    private final CountDownLatch failure = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setIncludeEventTypes(EVT_CACHE_REBALANCE_OBJECT_LOADED)
            .setCacheConfiguration(
                new CacheConfiguration()
                    .setName(DEFAULT_CACHE_NAME)
                    .setRebalanceDelay(100)
                    .setCacheMode(CacheMode.REPLICATED))
            .setFailureHandler((i, f) -> {
                failure.countDown();

                return true;
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceUncheckedError() throws Exception {
        startGrid(0).getOrCreateCache(DEFAULT_CACHE_NAME).put(1,1);

        CountDownLatch latch = new CountDownLatch(1);

        startGrid(1).events().localListen(e -> {
            latch.countDown();

            throw new Error();
        }, EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED);

        assertTrue("Exception was not thrown.", latch.await(3, TimeUnit.SECONDS));
        assertTrue("Rebalancing does not handle unchecked exceptions by failure handler",
            failure.await(3, TimeUnit.SECONDS));
    }
}
