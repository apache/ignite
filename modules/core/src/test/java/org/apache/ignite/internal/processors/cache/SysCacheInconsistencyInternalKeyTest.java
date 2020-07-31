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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.GridTaskNameHashKey;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.db.RebalanceBlockingSPI;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class SysCacheInconsistencyInternalKeyTest extends GridCommonAbstractTest {
    /** Slow rebalance cache name. */
    private static final String SLOW_REBALANCE_CACHE = UTILITY_CACHE_NAME;

    /** Supply message latch. */
    private static final AtomicReference<CountDownLatch> SUPPLY_MESSAGE_LATCH = new AtomicReference<>();

    /** Supply send latch. */
    private static final AtomicReference<CountDownLatch> SUPPLY_SEND_LATCH = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setCommunicationSpi(new RebalanceBlockingSPI(SUPPLY_MESSAGE_LATCH, SLOW_REBALANCE_CACHE, SUPPLY_SEND_LATCH));

        return cfg;
    }

    /**
     * Checks that {@link GridCacheInternal} must be added to delete queue.
     */
    @Test
    public void restartLeadToProblemWithDeletedQueue() throws Exception {
        IgniteEx node1 = startGrid(0);
        IgniteInternalCache<Object, Object> utilityCache = node1.context().cache().utilityCache();

        for (int i = 0; i < 1000; i++)
            utilityCache.putAsync(new GridTaskNameHashKey(i), "Obj").get();

        CountDownLatch stopRebalanceLatch = new CountDownLatch(1);

        CountDownLatch readyToSndLatch = new CountDownLatch(1);

        SUPPLY_SEND_LATCH.set(readyToSndLatch);

        SUPPLY_MESSAGE_LATCH.set(stopRebalanceLatch);

        runAsync(() -> startGrid(1));

        readyToSndLatch.await();

        for (int i = 0; i < 1000; i++)
            utilityCache.remove(new GridTaskNameHashKey(i));

        stopRebalanceLatch.countDown();

        awaitPartitionMapExchange(true, true, null);

        assertFalse(idleVerify(node1, UTILITY_CACHE_NAME).hasConflicts());
    }
}
