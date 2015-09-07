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

    private static int TEST_SIZE = 1_000_000;

    /** cache name. */
    protected static String CACHE_NAME_DHT = "cache";

    /** cache 2 name. */
    protected static String CACHE_2_NAME_DHT = "cache2";

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

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setName(CACHE_NAME_DHT);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        //cacheCfg.setRebalanceBatchSize(1024);
        //cacheCfg.setRebalanceBatchesCount(1);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg.setBackups(1);

        CacheConfiguration<Integer, Integer> cacheCfg2 = new CacheConfiguration<>();

        cacheCfg2.setName(CACHE_2_NAME_DHT);
        cacheCfg2.setCacheMode(CacheMode.PARTITIONED);
        //cacheCfg2.setRebalanceBatchSize(1024);
        //cacheCfg2.setRebalanceBatchesCount(1);
        cacheCfg2.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg2.setBackups(1);

        iCfg.setRebalanceThreadPoolSize(4);
        iCfg.setCacheConfiguration(cacheCfg, cacheCfg2);
        return iCfg;
    }

    /**
     * @param ignite Ignite.
     */
    protected void generateData(Ignite ignite) {
        try (IgniteDataStreamer<Integer, Integer> stmr = ignite.dataStreamer(CACHE_NAME_DHT)) {
            for (int i = 0; i < TEST_SIZE; i++) {
                if (i % 1_000_000 == 0)
                    log.info("Prepared " + i / 1_000_000 + "m entries.");

                stmr.addData(i, i);
            }

            stmr.flush();
        }
        try (IgniteDataStreamer<Integer, Integer> stmr = ignite.dataStreamer(CACHE_2_NAME_DHT)) {
            for (int i = 0; i < TEST_SIZE; i++) {
                if (i % 1_000_000 == 0)
                    log.info("Prepared " + i / 1_000_000 + "m entries.");

                stmr.addData(i, i + 3);
            }

            stmr.flush();
        }
    }

    /**
     * @param ignite Ignite.
     * @throws IgniteCheckedException
     */
    protected void checkData(Ignite ignite) throws IgniteCheckedException {
        for (int i = 0; i < TEST_SIZE; i++) {
            if (i % 1_000_000 == 0)
                log.info("Checked " + i / 1_000_000 + "m entries.");

            assert ignite.cache(CACHE_NAME_DHT).get(i) != null && ignite.cache(CACHE_NAME_DHT).get(i).equals(i) :
                "key " + i + " does not match (" + ignite.cache(CACHE_NAME_DHT).get(i) + ")";
        }
        for (int i = 0; i < TEST_SIZE; i++) {
            if (i % 1_000_000 == 0)
                log.info("Checked " + i / 1_000_000 + "m entries.");

            assert ignite.cache(CACHE_2_NAME_DHT).get(i) != null && ignite.cache(CACHE_2_NAME_DHT).get(i).equals(i + 3) :
                "key " + i + " does not match (" + ignite.cache(CACHE_2_NAME_DHT).get(i) + ")";
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

        new Thread(){
            @Override public void run() {
                try {
                    startGrid(1);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();

        U.sleep(500);

        new Thread(){
            @Override public void run() {
                try {
                    startGrid(2);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();// Should cancel current rebalancing.

        U.sleep(500);

        new Thread(){
            @Override public void run() {
                try {
                    startGrid(3);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();// Should cancel current rebalancing.

        U.sleep(500);

        new Thread(){
            @Override public void run() {
                try {
                    startGrid(4);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();// Should cancel current rebalancing.

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
    }
}