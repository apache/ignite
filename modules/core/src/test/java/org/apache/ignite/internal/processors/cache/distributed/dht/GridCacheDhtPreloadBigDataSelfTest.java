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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheRebalanceMode;
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

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_REBALANCE_BATCH_SIZE;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;

/**
 * Test large cache counts.
 */
public class GridCacheDhtPreloadBigDataSelfTest extends GridCommonAbstractTest {
    /** Size of values in KB. */
    private static final int KBSIZE = 10 * 1024;

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

    /**
     *
     */
    public GridCacheDhtPreloadBigDataSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setRebalanceBatchSize(preloadBatchSize);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setRebalanceMode(preloadMode);
        cc.setAffinity(new RendezvousAffinityFunction(false, partitions));
        cc.setBackups(backups);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        if (lbean != null)
            c.setLifecycleBeans(lbean);

        c.setDiscoverySpi(disco);
        c.setCacheConfiguration(cc);
        c.setDeploymentMode(CONTINUOUS);
        c.setNetworkTimeout(1000);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
        preloadMode = ASYNC;
        preloadBatchSize = DFLT_BATCH_SIZE;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // Clean up memory for test suite.
        lbean = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLargeObjects() throws Exception {
        preloadMode = SYNC;

        try {
            startGrid(0);

            int cnt = 10000;

            populate(grid(0).<Integer, byte[]>cache(null), cnt, KBSIZE);

            int gridCnt = 3;

            for (int i = 1; i < gridCnt; i++)
                startGrid(i);

            Thread.sleep(10000);

            for (int i = 0; i < gridCnt; i++) {
                IgniteCache<Integer, String> c = grid(i).cache(null);

                if (backups + 1 <= gridCnt)
                    assert c.localSize() < cnt : "Cache size: " + c.localSize();
                else
                    assert c.localSize() == cnt;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLargeObjectsWithLifeCycleBean() throws Exception {
        preloadMode = SYNC;
        partitions = 23;

        try {
            final int cnt = 10000;

            lbean = new LifecycleBean() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public void onLifecycleEvent(LifecycleEventType evt) {
                    if (evt == LifecycleEventType.AFTER_NODE_START) {
                        IgniteCache<Integer, byte[]> c = ignite.cache(null);

                        if (c.putIfAbsent(-1, new byte[1])) {
                            populate(c, cnt, KBSIZE);

                            info(">>> POPULATED GRID <<<");
                        }
                    }
                }
            };

            int gridCnt = 3;

            for (int i = 0; i < gridCnt; i++)
                startGrid(i);

            for (int i = 0; i < gridCnt; i++)
                info("Grid size [i=" + i + ", size=" + grid(i).cache(null).size() + ']');

            Thread.sleep(10000);

            for (int i = 0; i < gridCnt; i++) {
                IgniteCache<Integer, String> c = grid(i).cache(null);

                if (backups + 1 <= gridCnt)
                    assert c.localSize() < cnt;
                else
                    assert c.localSize() == cnt;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param c Cache.
     * @param cnt Key count.
     * @param kbSize Size in KB.
     * @throws IgniteCheckedException If failed.
     */
    private void populate(IgniteCache<Integer, byte[]> c, int cnt, int kbSize) {
        for (int i = 0; i < cnt; i++)
            c.put(i, value(kbSize));
    }

    /**
     * @param size Size.
     * @return Value.
     */
    private byte[] value(int size) {
        return new byte[size];
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 6 * 60 * 1000; // 6 min.
    }
}