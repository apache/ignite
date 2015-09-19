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

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheTopologySafeGetSelfTest extends GridCommonAbstractTest {
    /** Number of initial grids. */
    public static final int GRID_CNT = 4;

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** TX commit latch. */
    private CountDownLatch releaseLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(
            cacheCfg("tx", TRANSACTIONAL, false),
            cacheCfg("atomic", ATOMIC, false),
            cacheCfg("tx_near", TRANSACTIONAL, true),
            cacheCfg("atomic_near", ATOMIC, true));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @param name Cache name.
     * @param cacheMode Cache mode.
     * @param near Near enabled flag.
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheCfg(String name, CacheAtomicityMode cacheMode, boolean near) {
        CacheConfiguration cfg = new CacheConfiguration(name);

        cfg.setAtomicityMode(cacheMode);
        cfg.setBackups(1);

        if (near)
            cfg.setNearConfiguration(new NearCacheConfiguration());
        else
            cfg.setNearConfiguration(null);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTopologySafeNodeJoin() throws Exception {
        checkGetTopologySafeNodeJoin(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTopologySafeNodeJoinPrimaryLeave() throws Exception {
        checkGetTopologySafeNodeJoin(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void checkGetTopologySafeNodeJoin(boolean failPrimary) throws Exception {
        startGrids(GRID_CNT);

        awaitPartitionMapExchange();

        try {
            ClusterNode targetNode = ignite(1).cluster().localNode();

            info(">>> Target node: " + targetNode.id());

            // Populate caches with a key that does not belong to ignite(0).
            int key = -1;
            for (int i = 0; i < 100; i++) {
                Collection<ClusterNode> nodes = ignite(0).affinity("tx").mapKeyToPrimaryAndBackups(i);
                ClusterNode primaryNode = F.first(nodes);

                if (!nodes.contains(ignite(0).cluster().localNode()) && primaryNode.id().equals(targetNode.id())) {
                    ignite(1).cache("tx").put(i, i);
                    ignite(1).cache("atomic").put(i, i);
                    ignite(1).cache("tx_near").put(i, i);
                    ignite(1).cache("atomic_near").put(i, i);

                    key = i;


                    break;
                }
            }

            assertTrue(key != -1);

            IgniteInternalFuture<?> txFut = startBlockingTxAsync();

            IgniteInternalFuture<?> nodeFut = startNodeAsync();

            if (failPrimary)
                stopGrid(1);

            assertEquals(key, ((IgniteKernal)ignite(0)).internalCache("tx").getTopologySafe(key));
            assertEquals(key, ((IgniteKernal)ignite(0)).internalCache("atomic").getTopologySafe(key));
            assertEquals(key, ((IgniteKernal)ignite(0)).internalCache("tx_near").getTopologySafe(key));
            assertEquals(key, ((IgniteKernal)ignite(0)).internalCache("atomic_near").getTopologySafe(key));

            releaseTx();

            txFut.get();
            nodeFut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @return Future.
     * @throws Exception If failed.
     */
    private IgniteInternalFuture<?> startNodeAsync() throws Exception {
        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                startGrid(GRID_CNT);

                return null;
            }
        });

        U.sleep(1000);

        return fut;
    }

    /**
     * @return TX release future.
     * @throws Exception If failed.
     */
    private IgniteInternalFuture<?> startBlockingTxAsync() throws Exception {
        final CountDownLatch lockLatch = new CountDownLatch(1);

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (Transaction ignore = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    for (int i = 0; i < 30; i++)
                        ignite(0).cache("tx").get("value-" + i);

                    releaseLatch = new CountDownLatch(1);

                    lockLatch.countDown();

                    releaseLatch.await();
                }

                return null;
            }
        });

        lockLatch.await();

        return fut;
    }

    /**
     *
     */
    private void releaseTx() {
        assert releaseLatch != null;

        releaseLatch.countDown();
    }
}