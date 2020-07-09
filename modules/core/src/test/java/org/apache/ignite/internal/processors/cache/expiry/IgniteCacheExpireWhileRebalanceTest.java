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
package org.apache.ignite.internal.processors.cache.expiry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Expiration while writing and rebalancing.
 */
public class IgniteCacheExpireWhileRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static final int ENTRIES = 100000;

    /** */
    private static final int CLUSTER_SIZE = 4;

    /**
     * Finder.
     */
    protected static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi) cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler());

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 1)));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExpireWhileRebalancing() throws Exception {
        startGridsMultiThreaded(CLUSTER_SIZE);

        IgniteCache<Object, Object> cache = ignite(0).cache(DEFAULT_CACHE_NAME);

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            for (int i = 1; i <= ENTRIES; i++) {
                cache.put(i, i);

                if (i % (ENTRIES / 10) == 0)
                    System.out.println(">>> Entries put: " + i);
            }
            latch.countDown();
        }).start();

        stopGrid(CLUSTER_SIZE - 1);

        awaitPartitionMapExchange();

        startGrid(CLUSTER_SIZE - 1);

        latch.await(10, TimeUnit.SECONDS);

        int resultingSize = cache.size(CachePeekMode.PRIMARY);

        System.out.println(">>> Resulting size: " + resultingSize);

        assertTrue(resultingSize > 0);

        // Eviction started
        assertTrue(resultingSize < ENTRIES * 10 / 11);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }
}
