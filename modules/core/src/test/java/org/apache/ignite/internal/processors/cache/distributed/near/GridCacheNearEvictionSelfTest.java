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
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Tests entries distribution between primary-backup-near caches according to nodes count in grid.
 */
public class GridCacheNearEvictionSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private int gridCnt;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setDistributionMode(NEAR_PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setBackups(1);
        cc.setRebalanceMode(SYNC);
        cc.setNearEvictionPolicy(null);
        cc.setAtomicityMode(atomicityMode());
        cc.setAtomicWriteOrderMode(PRIMARY);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setMarshaller(new OptimizedMarshaller(false));

        return c;
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** @throws Exception If failed. */
    public void testNearEnabledOneNode() throws Exception {
        gridCnt = 1;

        startGridsMultiThreaded(gridCnt);

        try {
            IgniteCache<Integer, String> c = grid(0).jcache(null);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                c.put(i, Integer.toString(i));

            assertEquals(cnt, c.size());
            assertEquals(cnt, c.size());
            assertEquals(0, near(0).nearSize());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testNearEnabledTwoNodes() throws Exception {
        gridCnt = 2;

        startGridsMultiThreaded(gridCnt);

        try {
            final int cnt = 100;

            grid(0).compute().broadcast(new IgniteCallable<Object>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Object call() throws Exception {
                    IgniteCache<Integer, String> c = ignite.jcache(null);

                    for (int i = 0; i < cnt; i++)
                        c.put(i, Integer.toString(i));

                    return true;
                }
            });

            for (int i = 0; i < gridCnt; i++) {
                assertEquals(cnt, internalCache(i).size());
                assertEquals(0, near(i).nearSize());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testNearEnabledThreeNodes() throws Exception {
        gridCnt = 3;

        startGridsMultiThreaded(gridCnt);

        try {
            final int cnt = 100;

            grid(0).compute().broadcast(new IgniteCallable<Object>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Object call() throws Exception {
                    IgniteCache<Integer, String> c = ignite.jcache(null);

                    for (int i = 0; i < cnt; i++)
                        c.put(i, Integer.toString(i));

                    return true;
                }
            });

            for (int i = 0; i < gridCnt; i++) {
                final GridCache cache = internalCache(i);

                // Repeatedly check cache sizes because of concurrent cache updates.
                assertTrue(GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        // Every node contains either near, backup, or primary.
                        return cnt == cache.size();
                    }
                }, getTestTimeout()));

                int keySize = near(i).nearSize();

                assert keySize < cnt : "Key size is not less than count [cnt=" + cnt + ", size=" + keySize + ']';
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
