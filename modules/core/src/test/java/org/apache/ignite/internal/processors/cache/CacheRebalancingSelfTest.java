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
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

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
     * @throws Exception If failed.
     */
    public void testDisableRebalancing() throws Exception {
        IgniteEx ig0 = startGrid(0);

        IgniteEx ig1 = startGrid(1);

        IgniteEx ig2 = startGrid(2);

        IgniteEx ig3 = startGrid(3);

        System.out.println("ig0=" + ig0.localNode().id());
        System.out.println("ig1=" + ig1.localNode().id());
        System.out.println("ig2=" + ig2.localNode().id());
        System.out.println("ig3=" + ig3.localNode().id());


        String cacheName = "disableRebalanceTestCache";

        CacheConfiguration<Integer,Integer> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setBackups(1);
        cacheCfg.setName(cacheName);

        IgniteCache<Integer, Integer> cache0 = ig0.getOrCreateCache(cacheCfg);

        IgniteCache<Integer, Integer> cache1 = ig1.getOrCreateCache(cacheCfg);


        awaitPartitionMapExchange();

        Integer before0 = ig0.affinity(cacheName).primaryPartitions(ig0.localNode()).length;
        Integer before1 = ig1.affinity(cacheName).primaryPartitions(ig1.localNode()).length;

        ig1.rebalanceEnabled(false);

        Random r = new Random();
        for (int i=0;i<10240;i++)
            cache0.put(r.nextInt(), 1);

        stopGrid(2);

        IgniteFuture fut2 = cache0.rebalance();

        fut2.get();

        awaitPartitionMapExchange();
        Thread.sleep(9000);

        Integer after0 = ig0.affinity(cacheName).primaryPartitions(ig0.localNode()).length;
        Integer after1 = ig1.affinity(cacheName).primaryPartitions(ig1.localNode()).length;

        //ig0.affinity(DEFAULT_CACHE_NAME).mapPartitionsToNodes()

        assert before0 < after0;
        assert before1 == after1;

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
