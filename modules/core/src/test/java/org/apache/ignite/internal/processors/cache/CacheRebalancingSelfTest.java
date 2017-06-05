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
 *
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Test for rebalancing.
 */
public class CacheRebalancingSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalanceFuture() throws Exception {
        IgniteEx ig0 = startGrid(0);

        startGrid(1);

        IgniteCache<Object, Object> cache = ig0.cache(DEFAULT_CACHE_NAME);

        IgniteFuture fut1 = cache.rebalance();

        fut1.get();

        startGrid(2);

        IgniteFuture fut2 = cache.rebalance();

        assert internalFuture(fut2) != internalFuture(fut1);

        fut2.get();
    }


    /**
     * Test local cache size with and without rebalancing in case or topology change
     *
     * @throws Exception If failed.
     */
    public void testDisableRebalancing() throws Exception {
        IgniteEx ig0 = startGrid(0);

        IgniteEx ig1 = startGrid(1);

        IgniteEx ig2 = startGrid(2);

        CacheConfiguration<Integer,Integer> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setBackups(1);
        cacheCfg.setName("disableRebalanceTestCache");

        IgniteCache<Integer, Integer> cache0 = ig0.getOrCreateCache(cacheCfg);

        IgniteCache<Integer, Integer> cache1 = ig1.getOrCreateCache(cacheCfg);

        awaitPartitionMapExchange();

        /*
        Disable rebalansing on node1, populate cache, tear down node2 and test that no new entries come to node1
         */
        ig1.rebalanceEnabled(false);

        Random r = new Random();
        Map<Integer, Integer> cacheTestMap = new HashMap<>();
        for (int i=0;i<10240;i++){
            int k = r.nextInt();

            cache0.put(k, 1);

            cacheTestMap.put(k, 1);
        }


        int before0 = cache0.localSize(CachePeekMode.ALL);
        int before1 = cache1.localSize(CachePeekMode.ALL);

        stopGrid(2);

        for (Map.Entry<Integer, Integer> testEntry : cacheTestMap.entrySet()) {
            assert testEntry.getValue().equals(cache0.get(testEntry.getKey())) : "get " + cache0.get(testEntry.getKey())
                + " expect " + testEntry.getValue();
        }

        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return before0 < cache0.localSize(CachePeekMode.ALL);
            }
        }, 10_000): "ig0 local size  before= " + before0 + ", afterDisable = "
            + cache0.localSize(CachePeekMode.ALL);

        Thread.sleep(1500);

        int afterDisable0 = cache0.localSize(CachePeekMode.ALL);
        int afterDisable1 = cache1.localSize(CachePeekMode.ALL);

        assert before1 == afterDisable1: "ig1 local size  before= " + before1 + ", afterDisable = " + afterDisable1 +
            "(should be the same)";

        // Enable rebalansing on node1 and test that new entries come to node1
        ig1.rebalanceEnabled(true);


        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return afterDisable1 < cache1.localSize(CachePeekMode.ALL);
            }
        }, 10_000): "ig1 local size  afterDisable = " + afterDisable1 + ", afterEnable = "
            + cache1.localSize(CachePeekMode.ALL) + " (but should be greater)";

        Thread.sleep(1500);

        int afterEnable0 = cache0.localSize(CachePeekMode.ALL);

        assert afterDisable0 == afterEnable0 : "ig0 local size  afterDisable = " + afterDisable0 + ", afterEnable = "
            + afterEnable0;

        for (Map.Entry<Integer, Integer> testEntry : cacheTestMap.entrySet()) {
            assert testEntry.getValue().equals(cache0.get(testEntry.getKey())) : "get " + cache0.get(testEntry.getKey())
                + " expect " + testEntry.getValue();
        }
    }

    /**
     * @param fut Future.
     * @return Internal future.
     */
    private static IgniteInternalFuture internalFuture(IgniteFuture fut) {
        assert fut instanceof IgniteFutureImpl : fut;

        return ((IgniteFutureImpl) fut).internalFuture();
    }
}
