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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.PartitionLossPolicy.IGNORE;
import static org.apache.ignite.testframework.GridTestUtils.mergeExchangeWaitVersion;

/**
 * Tests if lost partitions are same on left nodes after other owners removal.
 */
public class CachePartitionLossDetectionOnNodeLeftTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setAtomicityMode(TRANSACTIONAL).
            setCacheMode(PARTITIONED).
            setPartitionLossPolicy(IGNORE). // If default will ever change...
            setBackups(0));

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testPartitionLossDetectionOnNodeLeft() throws Exception {
        try {
            final Ignite srv0 = startGrids(5);

            List<Integer> lost0 = Collections.synchronizedList(new ArrayList<>());
            List<Integer> lost1 = Collections.synchronizedList(new ArrayList<>());

            grid(0).events().localListen(evt -> {
                lost0.add(((CacheRebalancingEvent)evt).partition());

                return true;
            }, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

            grid(1).events().localListen(evt -> {
                lost1.add(((CacheRebalancingEvent)evt).partition());

                return true;
            }, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

            awaitPartitionMapExchange();

            mergeExchangeWaitVersion(srv0, 8, null);

            int[] p2 = srv0.affinity(DEFAULT_CACHE_NAME).primaryPartitions(grid(2).localNode());
            int[] p3 = srv0.affinity(DEFAULT_CACHE_NAME).primaryPartitions(grid(3).localNode());
            int[] p4 = srv0.affinity(DEFAULT_CACHE_NAME).primaryPartitions(grid(4).localNode());

            List<Integer> expLostParts = new ArrayList<>();

            for (int i = 0; i < p2.length; i++)
                expLostParts.add(p2[i]);
            for (int i = 0; i < p3.length; i++)
                expLostParts.add(p3[i]);
            for (int i = 0; i < p4.length; i++)
                expLostParts.add(p4[i]);

            Collections.sort(expLostParts);

            stopGrid(getTestIgniteInstanceName(4), true, false);
            stopGrid(getTestIgniteInstanceName(3), true, false);
            stopGrid(getTestIgniteInstanceName(2), true, false);

            waitForReadyTopology(internalCache(1, DEFAULT_CACHE_NAME).context().topology(), new AffinityTopologyVersion(8, 0));

            assertEquals("Node0", S.compact(expLostParts), S.compact(lost0));
            assertEquals("Node1", S.compact(expLostParts), S.compact(lost1));
        }
        finally {
            stopAllGrids();
        }
    }
}
