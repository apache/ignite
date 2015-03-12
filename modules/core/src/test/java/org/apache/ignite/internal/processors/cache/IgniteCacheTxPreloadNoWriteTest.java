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
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 *
 */
public class IgniteCacheTxPreloadNoWriteTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(REPLICATED);
        ccfg.setDistributionMode(PARTITIONED_ONLY);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setRebalanceMode(CacheRebalanceMode.ASYNC);
        ccfg.setAffinity(new CacheRendezvousAffinityFunction(false, 100));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoWrite() throws Exception {
        Ignite ignite0 = startGrid(0);

        CacheAffinity<Integer> aff = ignite0.affinity(null);

        IgniteCache<Integer, Object> cache0 = ignite0.jcache(null);

        for (int i = 0; i < 1000; i++)
            cache0.put(i + 10000, new byte[1024]);

        Ignite ignite1 = startGrid(1);

        Integer key = 70;

        // Want test scenario when ignite1 is new primary node, but ignite0 is still partition owner.
        assertTrue(aff.isPrimary(ignite1.cluster().localNode(), key));

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache0.get(key);

            tx.commit();
        }

        GridCacheAdapter cacheAdapter = ((IgniteKernal)ignite(0)).context().cache().internalCache();

        // Check all transactions are finished.
        assertEquals(0, cacheAdapter.context().tm().idMapSize());

        // Try to start one more node.
        startGrid(2);
    }
}
