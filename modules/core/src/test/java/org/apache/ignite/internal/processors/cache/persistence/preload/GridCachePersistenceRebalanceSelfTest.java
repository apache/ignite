/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.preload;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for checking cancellation rebalancing process if some events occurs.
 */
public class GridCachePersistenceRebalanceSelfTest extends GridCommonAbstractTest {
    /** */
    @Before
    public void setBefore() throws Exception {
        cleanPersistenceDir();

        System.setProperty(IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, "true");
    }

    /** */
    @After
    public void setAfter() {
        System.setProperty(IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, "false");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        .setMaxSize(100L * 1024 * 1024)
                        .setPersistenceEnabled(true))
                    .setWalMode(WALMode.LOG_ONLY))
            .setCacheConfiguration(
                new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
                    .setCacheMode(CacheMode.REPLICATED)
                    .setRebalanceMode(CacheRebalanceMode.ASYNC)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setBackups(1)
                    .setAffinity(new RendezvousAffinityFunction(false)
                        .setPartitions(8)));
    }

    /** */
    @Test
    public void testClientNodeJoinAtRebalancing() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 2048; i++)
            cache.put(i, new byte[2048]);

        awaitPartitionMapExchange();

        log.info("##### node 2");

        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(false);

        ignite0.cluster().active(true);

        assert ignite0.cluster().active();

        awaitPartitionMapExchange();
    }
}
