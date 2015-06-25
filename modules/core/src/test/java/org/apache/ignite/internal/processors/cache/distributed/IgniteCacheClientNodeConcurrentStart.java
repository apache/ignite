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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheRebalanceMode.*;

/**
 *
 */
public class IgniteCacheClientNodeConcurrentStart extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES_CNT = 6;

    /** */
    private Set<Integer> clientNodes;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        assertNotNull(clientNodes);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        boolean client = false;

        for (Integer clientIdx : clientNodes) {
            if (getTestGridName(clientIdx).equals(gridName)) {
                client = true;

                break;
            }
        }

        cfg.setClientMode(client);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setBackups(0);
        ccfg.setRebalanceMode(SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStart() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 3; i++) {
            try {
                clientNodes = new HashSet<>();

                while (clientNodes.size() < 2)
                    clientNodes.add(rnd.nextInt(1, NODES_CNT));

                clientNodes.add(NODES_CNT - 1);

                log.info("Test iteration [iter=" + i + ", clients=" + clientNodes + ']');

                Ignite srv = startGrid(0); // Start server node first.

                assertFalse(srv.configuration().isClientMode());

                startGridsMultiThreaded(1, NODES_CNT - 1);

                checkTopology(NODES_CNT);

                awaitPartitionMapExchange();

                for (int node : clientNodes) {
                    Ignite ignite = grid(node);

                    assertTrue(ignite.configuration().isClientMode());
                }
            }
            finally {
                stopAllGrids();
            }
        }
    }
}
