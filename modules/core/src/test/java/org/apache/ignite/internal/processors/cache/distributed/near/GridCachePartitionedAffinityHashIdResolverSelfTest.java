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
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Partitioned affinity hash ID resolver self test.
 */
public class GridCachePartitionedAffinityHashIdResolverSelfTest extends GridCommonAbstractTest {
    /** Shared IP finder. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Hash ID resolver. */
    private AffinityNodeHashResolver rslvr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction();

        aff.setHashIdResolver(rslvr);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAffinity(aff);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test when there is duplicate hash IDs.
     *
     * @throws Exception If failed.
     */
    public void testDuplicateId() throws Exception {
        rslvr = new BogusHashResolver();

        startGrid(0);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                startGrid(1);

                return null;
            }
        }, IgniteCheckedException.class, "Failed to start manager: GridManagerAdapter [enabled=true, name=" +
            "org.apache.ignite.internal.managers.discovery.GridDiscoveryManager]");
    }

    /**
     *
     */
    private static class BogusHashResolver implements AffinityNodeHashResolver {
        /** {@inheritDoc} */
        @Override public Object resolve(ClusterNode node) {
            return 1;
        }
    }
}
