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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;

/**
 *
 */
public class IgniteCacheServerNodeConcurrentStart extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int ITERATIONS = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinderCleanFrequency(getTestTimeout() * 2);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName("cache-1");
        ccfg1.setCacheMode(REPLICATED);
        ccfg1.setRebalanceMode(SYNC);

        CacheConfiguration ccfg2 = new CacheConfiguration();

        ccfg2.setName("cache-2");
        ccfg2.setCacheMode(PARTITIONED);
        ccfg2.setRebalanceMode(SYNC);
        ccfg2.setBackups(2);

        CacheConfiguration ccfg3 = new CacheConfiguration();

        ccfg3.setName("cache-3");
        ccfg3.setCacheMode(PARTITIONED);
        ccfg3.setRebalanceMode(SYNC);
        ccfg3.setBackups(0);

        cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return ITERATIONS * 3 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStart() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            log.info("Iteration: " + i);

            long start = System.currentTimeMillis();

            startGridsMultiThreaded(10, false);

            startGridsMultiThreaded(10, 10);

            awaitPartitionMapExchange();

            stopAllGrids();

            log.info("Iteration finished, time: " + (System.currentTimeMillis() - start) / 1000f);
        }
    }
}
