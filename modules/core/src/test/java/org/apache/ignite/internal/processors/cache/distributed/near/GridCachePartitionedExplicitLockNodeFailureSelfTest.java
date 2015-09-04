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
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Tests for node failure in transactions.
 */
public class GridCachePartitionedExplicitLockNodeFailureSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final int GRID_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setBackups(GRID_CNT - 1);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setNearConfiguration(new NearCacheConfiguration());

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** @throws Exception If check failed. */
    @SuppressWarnings("ErrorNotRethrown")
    public void testLockFromNearOrBackup() throws Exception {
        startGrids(GRID_CNT);

        int idx = 0;

        info("Grid will be stopped: " + idx);

        Integer key = 0;

        while (grid(idx).cluster().mapKeyToNode(null, key).id().equals(grid(0).localNode().id()))
            key++;

        ClusterNode node = grid(idx).cluster().mapKeyToNode(null, key);

        info("Primary node for key [id=" + node.id() + ", order=" + node.order() + ", key=" + key + ']');

        IgniteCache<Integer, String> cache = jcache(idx);

        cache.put(key, "val");

        Lock lock = cache.lock(key);

        assert lock.tryLock();

        for (int checkIdx = 1; checkIdx < GRID_CNT; checkIdx++) {
            info("Check grid index: " + checkIdx);

            IgniteCache<Integer, String> checkCache = jcache(checkIdx);

            assert !checkCache.lock(key).tryLock();
        }

        Collection<IgniteFuture<?>> futs = new LinkedList<>();

        for (int i = 1; i < GRID_CNT; i++) {
            futs.add(
                waitForLocalEvent(grid(i).events(), new P1<Event>() {
                    @Override public boolean apply(Event e) {
                        info("Received grid event: " + e);

                        return true;
                    }
                }, EVT_NODE_LEFT));
        }

        stopGrid(idx);

        for (IgniteFuture<?> fut : futs)
            fut.get();

        for (int i = 0; i < 3; i++) {
            try {
                for (int checkIdx = 1; checkIdx < GRID_CNT; checkIdx++) {
                    info("Check grid index: " + checkIdx);

                    IgniteCache<Integer, String> checkCache = jcache(checkIdx);

                    assert !checkCache.isLocalLocked(key, false);
                }
            }
            catch (AssertionError e) {
                if (i == 2)
                    throw e;

                U.warn(log, "Check failed (will retry in 1000 ms): " + e.getMessage());

                U.sleep(1000);

                continue;
            }

            // Check passed on all grids.
            break;
        }
    }
}