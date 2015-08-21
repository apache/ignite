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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridCacheNearTxForceKeyTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(ASYNC);
        ccfg.setRebalanceDelay(5000);
        ccfg.setBackups(0);
        ccfg.setNearConfiguration(new NearCacheConfiguration());

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Test provokes scenario when primary node sends force key request to node started transaction.
     *
     * @throws Exception If failed.
     */
    public void testNearTx() throws Exception {
        Ignite ignite0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = ignite0.cache(null);

        Ignite ignite1 = startGrid(1);

        final Integer key = 2;

        assertNull(cache.getAndPut(key, key));

        assertTrue(ignite0.affinity(null).isPrimary(ignite1.cluster().localNode(), key));
    }
}
