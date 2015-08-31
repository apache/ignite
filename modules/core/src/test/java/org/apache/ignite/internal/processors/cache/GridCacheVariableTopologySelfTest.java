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

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Affinity routing tests.
 */
public class GridCacheVariableTopologySelfTest extends GridCommonAbstractTest {
    /** */
    private static final Random RAND = new Random();

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Constructs test. */
    public GridCacheVariableTopologySelfTest() {
        super(/* don't start grid */ false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        // Default cache configuration.
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        assert G.allGrids().isEmpty();
    }

    /**
     * @param cnt Number of grids to starts.
     * @param startIdx Start grid index.
     * @throws Exception If failed to start grids.
     */
    private void startGrids(int cnt, int startIdx) throws Exception {
        assert startIdx >= 0;
        assert cnt >= 0;

        for (int idx = startIdx; idx < startIdx + cnt; idx++)
            startGrid(idx);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testNodeStop() throws Exception {
        // -- Test parameters. -- //
        int nodeCnt = 3;
        int threadCnt = 20;
        final int txCnt = 1000;
        final long txDelay = 0;
        final int keyRange = 1000;
        final int logMod = 20;

        assert nodeCnt > 1 : "Node count for this test must be greater than 1.";

        startGrids(nodeCnt, 0);

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CAX() {
            /** */
            private int cnt;

            @SuppressWarnings({"BusyWait"})
            @Override public void applyx() {
                while (cnt++ < txCnt && !done.get()) {
                    IgniteCache<Object, Object> cache = grid(0).cache(null);

                    if (cnt % logMod == 0)
                        info("Starting transaction: " + cnt);

                    try (Transaction tx = grid(0).transactions().txStart()) {
                        int kv = RAND.nextInt(keyRange);

                        cache.put(kv, kv);

                        cache.get(kv);

                        tx.commit();
                    }
                    catch (ClusterTopologyException e) {
                        info("Caught topology exception: " + e);
                    }
                    catch (IgniteException e) {
                        if (X.hasCause(e, ClusterTopologyCheckedException.class))
                            info("Caught cache exception: " + e);
                        else
                            throw e;
                    }

                    try {
                        Thread.sleep(txDelay);
                    }
                    catch (InterruptedException ignored) {
                        // No-op.
                    }
                }
            }
        }, threadCnt, "TEST-THREAD");

        Thread.sleep(2000);

        for (int idx = 1; idx < nodeCnt; idx++) {
            info("Stopping node: " + idx);

            stopGrid(idx);
        }

        // This is just for debugging.
        /*
        GridFuture<?> debugFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @SuppressWarnings({"UnusedDeclaration"})
            @Override public void run() {
                Cache<Object, Object> cache = ((IgniteKernal)grid(0)).cache(null);

                try {
                    Thread.sleep(15000);
                }
                catch (InterruptedException ignored) {
                    return;
                }

                info("Set breakpoint here.");
            }
        }, 1, "TEST-THREAD");
        */

        done.set(true);

        fut.get();

        stopGrid(0);

        info("Grid 0 stopped.");

        //debugFut.get();
    }
}