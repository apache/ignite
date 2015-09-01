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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests rendezvous affinity function with CLIENT_ONLY node (GG-8768).
 */
public class GridCacheRendezvousAffinityClientSelfTest extends GridCommonAbstractTest {
    /** Client node. */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAffinity(new RendezvousAffinityFunction());

        if (client)
            cfg.setClientMode(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNode() throws Exception {
        try {
            client = true;

            startGrid(0);

            client = false;

            startGrid(1);
            startGrid(2);
            startGrid(3);

            Map<Integer, Collection<UUID>> mapping = new HashMap<>();

            for (int i = 0; i < 4; i++) {
                IgniteCache<Object, Object> cache = grid(i).cache(null);

                Affinity<Object> aff = affinity(cache);

                int parts = aff.partitions();

                for (int p = 0; p < parts; p++) {
                    Collection<ClusterNode> nodes = aff.mapPartitionToPrimaryAndBackups(p);

                    assertEquals(2, nodes.size());

                    Collection<UUID> cur = mapping.get(p);

                    if (cur == null)
                        mapping.put(p, F.nodeIds(nodes));
                    else {
                        Iterator<UUID> nodesIt = F.nodeIds(nodes).iterator();

                        for (UUID curNode : cur) {
                            UUID node = nodesIt.next();

                            assertEquals(curNode, node);
                        }
                    }
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }
}