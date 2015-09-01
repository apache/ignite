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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_REBALANCE_BATCH_SIZE;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;

/**
 * Test large cache counts.
 */
@SuppressWarnings({"BusyWait"})
public class GridCacheDhtPreloadUnloadSelfTest extends GridCommonAbstractTest {
    /** Default backups. */
    private static final int DFLT_BACKUPS = 1;

    /** Partitions. */
    private static final int DFLT_PARTITIONS = 521;

    /** Preload batch size. */
    private static final int DFLT_BATCH_SIZE = DFLT_REBALANCE_BATCH_SIZE;

    /** Number of key backups. Each test method can set this value as required. */
    private int backups = DFLT_BACKUPS;

    /** Preload mode. */
    private CacheRebalanceMode preloadMode = ASYNC;

    /** */
    private int preloadBatchSize = DFLT_BATCH_SIZE;

    /** Number of partitions. */
    private int partitions = DFLT_PARTITIONS;

    /** */
    private LifecycleBean lbean;

    /** IP finder. */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Network timeout. */
    private long netTimeout = 1000;

    /**
     *
     */
    public GridCacheDhtPreloadUnloadSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setRebalanceBatchSize(preloadBatchSize);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setRebalanceMode(preloadMode);
        cc.setAffinity(new RendezvousAffinityFunction(false, partitions));
        cc.setBackups(backups);
        cc.setAtomicityMode(TRANSACTIONAL);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        if (lbean != null)
            c.setLifecycleBeans(lbean);

        c.setDiscoverySpi(disco);
        c.setCacheConfiguration(cc);
        c.setDeploymentMode(CONTINUOUS);
        c.setNetworkTimeout(netTimeout);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
        preloadMode = ASYNC;
        preloadBatchSize = DFLT_BATCH_SIZE;
        netTimeout = 1000;
    }

    /** @throws Exception If failed. */
    public void testUnloadZeroBackupsTwoNodes() throws Exception {
        preloadMode = SYNC;
        backups = 0;
        netTimeout = 500;

        try {
            startGrid(0);

            int cnt = 1000;

            populate(grid(0).<Integer, String>cache(null), cnt);

            int gridCnt = 2;

            for (int i = 1; i < gridCnt; i++)
                startGrid(i);

            long wait = 3000;

            waitForUnload(gridCnt, cnt, wait);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testUnloadOneBackupTwoNodes() throws Exception {
        preloadMode = SYNC;
        backups = 1;
        netTimeout = 500;

        try {
            startGrid(0);

            int cnt = 1000;

            populate(grid(0).<Integer, String>cache(null), cnt);

            int gridCnt = 2;

            for (int i = 1; i < gridCnt; i++)
                startGrid(i);

            long wait = 2000;

            info("Sleeping for " + wait + "ms");

            // Unfortunately there is no other way but sleep.
            Thread.sleep(wait);

            for (int i = 0; i < gridCnt; i++)
                info("Grid size [i=" + i + ", size=" + grid(i).cache(null).localSize() + ']');

            for (int i = 0; i < gridCnt; i++) {
                IgniteCache<Integer, String> c = grid(i).cache(null);

                // Nothing should be unloaded since nodes are backing up each other.
                assertEquals(cnt, c.localSize(CachePeekMode.ALL));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     * @param gridCnt Grid count.
     * @param cnt Count.
     * @param wait Wait.
     * @throws InterruptedException If interrupted.
     */
    private void waitForUnload(long gridCnt, long cnt, long wait) throws InterruptedException {
        info("Waiting for preloading to complete for " + wait + "ms...");

        long endTime = System.currentTimeMillis() + wait;

        while (System.currentTimeMillis() < endTime) {
            boolean err = false;

            for (int i = 0; i < gridCnt; i++) {
                IgniteCache<Integer, String> c = grid(i).cache(null);

                if (c.localSize() >= cnt)
                    err = true;
            }

            if (!err)
                break;
            else
                Thread.sleep(500);
        }

        for (int i = 0; i < gridCnt; i++)
            info("Grid size [i=" + i + ", size=" + grid(i).cache(null).localSize() + ']');

        for (int i = 0; i < gridCnt; i++) {
            IgniteCache<Integer, String> c = grid(i).cache(null);

            assert c.localSize() < cnt;
        }
    }

    /** @throws Exception If failed. */
    public void testUnloadOneBackupThreeNodes() throws Exception {
        preloadMode = SYNC;
        backups = 1;
        netTimeout = 500;
        partitions = 23;

        try {
            startGrid(0);

            int cnt = 1000;

            populate(grid(0).<Integer, String>cache(null), cnt);

            int gridCnt = 3;

            for (int i = 1; i < gridCnt; i++) {
                startGrid(i);

                for (int j = 0; j <= i; j++)
                    info("Grid size [i=" + i + ", size=" + grid(j).cache(null).localSize() + ']');
            }

            long wait = 3000;

            waitForUnload(gridCnt, cnt, wait);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testUnloadOneBackThreeNodesWithLifeCycleBean() throws Exception {
        preloadMode = SYNC;
        backups = 1;

        try {
            final int cnt = 1000;

            lbean = new LifecycleBean() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public void onLifecycleEvent(LifecycleEventType evt) {
                    if (evt == LifecycleEventType.AFTER_NODE_START) {
                        IgniteCache<Integer, String> c = ignite.cache(null);

                        if (c.putIfAbsent(-1, "true")) {
                            populate(ignite.<Integer, String>cache(null), cnt);

                            info(">>> POPULATED GRID <<<");
                        }
                    }
                }
            };

            int gridCnt = 3;

            for (int i = 0; i < gridCnt; i++) {
                startGrid(i);

                for (int j = 0; j < i; j++)
                    info("Grid size [i=" + i + ", size=" + grid(j).cache(null).localSize() + ']');
            }

            long wait = 3000;

            waitForUnload(gridCnt, cnt, wait);
        }
        finally {
            lbean = null;

            stopAllGrids();
        }
    }

    /**
     * @param c Cache.
     * @param cnt Key count.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private void populate(IgniteCache<Integer, String> c, int cnt) {
        for (int i = 0; i < cnt; i++)
            c.put(i, value(1024));
    }

    /**
     * @param size Size.
     * @return Value.
     */
    private String value(int size) {
        StringBuilder b = new StringBuilder(size / 2 + 1);

        for (int i = 0; i < size / 3; i++)
            b.append('a' + (i % 26));

        return b.toString();
    }
}