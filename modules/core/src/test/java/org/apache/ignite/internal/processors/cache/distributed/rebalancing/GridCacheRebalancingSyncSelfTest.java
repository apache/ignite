/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridCacheRebalancingSyncSelfTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    private static int TEST_SIZE = 100_000;

    /** partitioned cache name. */
    protected static String CACHE_NAME_DHT_PARTITIONED = "cacheP";

    /** partitioned cache 2 name. */
    protected static String CACHE_NAME_DHT_PARTITIONED_2 = "cacheP2";

    /** replicated cache name. */
    protected static String CACHE_NAME_DHT_REPLICATED = "cacheR";

    /** replicated cache 2 name. */
    protected static String CACHE_NAME_DHT_REPLICATED_2 = "cacheR2";

    /** */
    private volatile boolean concurrentStartFinished = false;

    /** */
    private volatile boolean concurrentStartFinished2 = false;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setForceServerMode(true);

        if (getTestGridName(10).equals(gridName))
            iCfg.setClientMode(true);

        CacheConfiguration<Integer, Integer> cachePCfg = new CacheConfiguration<>();

        cachePCfg.setName(CACHE_NAME_DHT_PARTITIONED);
        cachePCfg.setCacheMode(CacheMode.PARTITIONED);
        cachePCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cachePCfg.setBackups(1);

        CacheConfiguration<Integer, Integer> cachePCfg2 = new CacheConfiguration<>();

        cachePCfg2.setName(CACHE_NAME_DHT_PARTITIONED_2);
        cachePCfg2.setCacheMode(CacheMode.PARTITIONED);
        cachePCfg2.setRebalanceMode(CacheRebalanceMode.SYNC);
        cachePCfg2.setBackups(1);

        CacheConfiguration<Integer, Integer> cacheRCfg = new CacheConfiguration<>();

        cacheRCfg.setName(CACHE_NAME_DHT_REPLICATED);
        cacheRCfg.setCacheMode(CacheMode.REPLICATED);
        cacheRCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheRCfg.setBackups(1);

        CacheConfiguration<Integer, Integer> cacheRCfg2 = new CacheConfiguration<>();

        cacheRCfg2.setName(CACHE_NAME_DHT_REPLICATED_2);
        cacheRCfg2.setCacheMode(CacheMode.REPLICATED);
        cacheRCfg2.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheRCfg2.setBackups(1);

        iCfg.setRebalanceThreadPoolSize(4);
        iCfg.setCacheConfiguration(cachePCfg, cachePCfg2, cacheRCfg, cacheRCfg2);
        return iCfg;
    }

    protected void generateData(Ignite ignite) {
        generateData(ignite, CACHE_NAME_DHT_PARTITIONED);
        generateData(ignite, CACHE_NAME_DHT_PARTITIONED_2);
        generateData(ignite, CACHE_NAME_DHT_REPLICATED);
        generateData(ignite, CACHE_NAME_DHT_REPLICATED_2);
    }

    /**
     * @param ignite Ignite.
     */
    protected void generateData(Ignite ignite, String name) {
        try (IgniteDataStreamer<Integer, Integer> stmr = ignite.dataStreamer(name)) {
            for (int i = 0; i < TEST_SIZE; i++) {
                if (i % (TEST_SIZE / 10) == 0)
                    log.info("Prepared " + i * 100 / (TEST_SIZE) + "% entries (" + TEST_SIZE + ").");

                stmr.addData(i, i + name.hashCode());
            }

            stmr.flush();
        }
    }

    protected void checkData(Ignite ignite) throws IgniteCheckedException {
        checkData(ignite, CACHE_NAME_DHT_PARTITIONED);
        checkData(ignite, CACHE_NAME_DHT_PARTITIONED_2);
        checkData(ignite, CACHE_NAME_DHT_REPLICATED);
        checkData(ignite, CACHE_NAME_DHT_REPLICATED_2);
    }

    /**
     * @param ignite Ignite.
     * @throws IgniteCheckedException
     */
    protected void checkData(Ignite ignite, String name) throws IgniteCheckedException {
        for (int i = 0; i < TEST_SIZE; i++) {
            if (i % (TEST_SIZE / 10) == 0)
                log.info("Checked " + i * 100 / (TEST_SIZE) + "% entries (" + TEST_SIZE + ").");

            assert ignite.cache(name).get(i) != null && ignite.cache(name).get(i).equals(i + name.hashCode()) :
                "value " + i + name.hashCode() + " does not match (" + ignite.cache(name).get(i) + ")";
        }
    }

    /**
     * @throws Exception
     */
    public void testSimpleRebalancing() throws Exception {
        Ignite ignite = startGrid(0);

        generateData(ignite);

        log.info("Preloading started.");

        long start = System.currentTimeMillis();

        startGrid(1);

        waitForRebalancing(1, 2);

        long spend = (System.currentTimeMillis() - start) / 1000;

        stopGrid(0);

        checkData(grid(1));

        log.info("Spend " + spend + " seconds to rebalance entries.");

        stopAllGrids();
    }

    /**
     * @param id Id.
     * @param top Topology.
     */
    protected void waitForRebalancing(int id, int top) throws IgniteCheckedException {
        boolean finished = false;

        while (!finished) {
            finished = true;

            for (GridCacheAdapter c : grid(id).context().cache().internalCaches()) {
                GridDhtPartitionDemander.SyncFuture fut = (GridDhtPartitionDemander.SyncFuture)c.preloader().syncFuture();
                if (fut.topologyVersion().topologyVersion() != top) {
                    finished = false;

                    break;
                }
                else
                    fut.get();
            }
        }
    }

    /**
     * @throws Exception
     */
    public void testComplexRebalancing() throws Exception {
        Ignite ignite = startGrid(0);

        generateData(ignite);

        log.info("Preloading started.");

        long start = System.currentTimeMillis();

        new Thread() {
            @Override public void run() {
                try {
                    startGrid(1);
                    startGrid(2);

                    concurrentStartFinished = true;
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        new Thread() {
            @Override public void run() {
                try {
                    startGrid(3);
                    startGrid(4);

                    concurrentStartFinished2 = true;
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();// Should cancel current rebalancing.

        while (!concurrentStartFinished || !concurrentStartFinished2) {
            U.sleep(10);
        }

        //wait until cache rebalanced in async mode
        waitForRebalancing(1, 5);
        waitForRebalancing(2, 5);
        waitForRebalancing(3, 5);
        waitForRebalancing(4, 5);

        //cache rebalanced in async node

        stopGrid(0);

        //wait until cache rebalanced
        waitForRebalancing(1, 6);
        waitForRebalancing(2, 6);
        waitForRebalancing(3, 6);
        waitForRebalancing(4, 6);

        //cache rebalanced

        stopGrid(1);

        //wait until cache rebalanced
        waitForRebalancing(2, 7);
        waitForRebalancing(3, 7);
        waitForRebalancing(4, 7);

        //cache rebalanced

        stopGrid(2);

        //wait until cache rebalanced
        waitForRebalancing(3, 8);
        waitForRebalancing(4, 8);

        //cache rebalanced

        stopGrid(3);

        long spend = (System.currentTimeMillis() - start) / 1000;

        checkData(grid(4));

        log.info("Spend " + spend + " seconds to rebalance entries.");

        stopAllGrids();
    }

    /**
     * @throws Exception
     */
    public void testBackwardCompatibility() throws Exception {
        Ignite ignite = startGrid(0);

        Map<String, Object> map = new HashMap<>(ignite.cluster().localNode().attributes());

        map.put(IgniteNodeAttributes.REBALANCING_VERSION, 0);

        ((TcpDiscoveryNode)ignite.cluster().localNode()).setAttributes(map);

        generateData(ignite);

        startGrid(1);

        waitForRebalancing(1, 2);

        stopGrid(0);

        checkData(grid(1));

        stopAllGrids();
    }
}