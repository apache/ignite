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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for checking cancellation rebalancing process if some events occurs.
 */
public class GridCacheRebalancingByPartitionsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String DHT_PARTITIONED_CACHE = "cacheP";

    /** */
    @Before
    public void setBefore() {
        System.setProperty(IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, "true");
    }

    /** */
    @After
    public void setAfter() {
        System.setProperty(IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED, "false");
    }

    /** */
    @Test
    public void testClientNodeJoinAtRebalancing() throws Exception {
        final IgniteEx ignite0 = startGrid(0);

        IgniteCache<Integer, Integer> cache = ignite0.createCache(
            new CacheConfiguration<Integer, Integer>(DHT_PARTITIONED_CACHE)
                .setCacheMode(CacheMode.PARTITIONED)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setBackups(1)
                .setRebalanceOrder(2)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setAffinity(new RendezvousAffinityFunction(false)));

        for (int i = 0; i < 2048; i++)
            cache.put(i, i);

        final IgniteEx ignite1 = startGrid(1);

        awaitPartitionMapExchange();
    }
}
