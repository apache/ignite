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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheFullScanIteratorSelfTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    private static final int GRID_CNT = 8;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000L;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setNearConfiguration(null);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setWriteSynchronizationMode(null);
        ccfg.setAtomicWriteOrderMode(null);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     *
     */
    public void testFullScanIterator() throws Exception {
        int iterCnt = 100;

        for (int i = 1; i <= iterCnt; i++) {
            System.out.println("*** Iteration " + i + "/" + iterCnt);

            startGrids(GRID_CNT);

            try {
                doTest();
            }
            catch (Exception e) {
                if (X.hasCause(e, CacheException.class)) {
                    e.printStackTrace();

                    fail();
                }
            }
            finally {
                stopAllGrids();
            }
        }
    }

    /**
     *
     */
    private void doTest() throws Exception {
        List<T2<Integer, UUID>> nodes = new ArrayList<>(GRID_CNT);

        for (int i = 0; i < GRID_CNT; i++)
            nodes.add(new T2<>(i, grid(i).localNode().id()));

        Collections.shuffle(nodes);

        IgniteCache<UUID, String> cache = grid(0).cache(null);

        for (T2<Integer, UUID> node : nodes)
            cache.put(node.get2(), node.get2().toString());

        Set<UUID> rmv = new HashSet<>();

        T2<Integer, UUID> prev = null;

        for (T2<Integer, UUID> node : nodes) {
            if (prev != null) {
                IgniteCache<UUID, String> cache2 = grid(node.get1()).cache(null);

                cache2.remove(prev.get2());

                rmv.add(prev.get2());

                System.out.println("Removed: " + prev.get1() + "/" + prev.get2());

                System.out.println("Query on: " + node.get1() + "/" + node.get2());

                for (Cache.Entry<UUID, String> entry : cache2) {
                    System.out.println(entry);

                    assertFalse(rmv.contains(entry.getKey()));
                }
            }

            grid(node.get1()).close();

            System.out.println("Killed: " + node.get1() + "/" + node.get2());

            prev = node;
        }
    }
}
