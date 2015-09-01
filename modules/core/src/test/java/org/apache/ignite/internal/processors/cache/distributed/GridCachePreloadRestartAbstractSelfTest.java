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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_REBALANCE_BATCH_SIZE;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;

/**
 * Test node restart.
 */
public abstract class GridCachePreloadRestartAbstractSelfTest extends GridCommonAbstractTest {
    /** Flag for debug output. */
    private static final boolean DEBUG = false;

    /** Cache name. */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** */
    private static final long TEST_TIMEOUT = 5 * 60 * 1000;

    /** Default backups. */
    private static final int DFLT_BACKUPS = 1;

    /** Partitions. */
    private static final int DFLT_PARTITIONS = 521;

    /** Preload batch size. */
    private static final int DFLT_BATCH_SIZE = DFLT_REBALANCE_BATCH_SIZE;

    /** Number of key backups. Each test method can set this value as required. */
    private int backups = DFLT_BACKUPS;

    /** */
    private static final int DFLT_NODE_CNT = 4;

    /** */
    private static final int DFLT_KEY_CNT = 100;

    /** */
    private static final int DFLT_RETRIES = 2;

    /** */
    private static volatile int idx = -1;

    /** Preload mode. */
    private CacheRebalanceMode preloadMode = ASYNC;

    /** */
    private int preloadBatchSize = DFLT_BATCH_SIZE;

    /** Number of partitions. */
    private int partitions = DFLT_PARTITIONS;

    /** Node count. */
    private int nodeCnt = DFLT_NODE_CNT;

    /** Key count. */
    private int keyCnt = DFLT_KEY_CNT;

    /** Retries. */
    private int retries = DFLT_RETRIES;

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        // Discovery.
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);
        c.setDiscoverySpi(disco);
        c.setDeploymentMode(CONTINUOUS);

        // Cache.
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName(CACHE_NAME);
        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setStartSize(20);
        cc.setRebalanceMode(preloadMode);
        cc.setRebalanceBatchSize(preloadBatchSize);
        cc.setAffinity(new RendezvousAffinityFunction(false, partitions));
        cc.setBackups(backups);
        cc.setAtomicityMode(TRANSACTIONAL);

        if (!nearEnabled())
            cc.setNearConfiguration(null);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return {@code True} if near cache is enabled.
     */
    protected abstract boolean nearEnabled();

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
        preloadMode = ASYNC;
        preloadBatchSize = DFLT_BATCH_SIZE;
        nodeCnt = DFLT_NODE_CNT;
        keyCnt = DFLT_KEY_CNT;
        retries = DFLT_RETRIES;
        idx = -1;

//        resetLog4j(Level.DEBUG, true,
//            // Categories.
//            GridDhtPreloader.class.getPackage().getName(),
//            GridDhtPartitionTopologyImpl.class.getName(),
//            GridDhtLocalPartition.class.getName());
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * @throws Exception If failed.
     */
    private void startGrids() throws  Exception {
        for (int i = 0; i < nodeCnt; i++) {
            startGrid(i);

            if (idx < 0)
                idx = i;
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void stopGrids() throws  Exception {
        for (int i = 0; i < nodeCnt; i++)
            stopGrid(i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSyncPreloadRestart() throws Exception {
        preloadMode = SYNC;

        checkRestart();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsyncPreloadRestart() throws Exception {
        preloadMode = ASYNC;

        checkRestart();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisabledPreloadRestart() throws Exception {
        preloadMode = NONE;

        checkRestart();
    }

    /**
     * @param c Cache projection.
     */
    private void affinityBeforeStop(IgniteCache<Integer, String> c) {
        for (int key = 0; key < keyCnt; key++) {
            int part = affinity(c).partition(key);

            info("Affinity nodes before stop [key=" + key + ", partition" + part + ", nodes=" +
                U.nodeIds(affinity(c).mapPartitionToPrimaryAndBackups(part)) + ']');
        }
    }

    /**
     * @param c Cache projection.
     */
    private void affinityAfterStart(IgniteCache<Integer, String> c) {
        if (DEBUG) {
            for (int key = 0; key < keyCnt; key++) {
                int part = affinity(c).partition(key);

                info("Affinity odes after start [key=" + key + ", partition" + part + ", nodes=" +
                    U.nodeIds(affinity(c).mapPartitionToPrimaryAndBackups(part)) + ']');
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkRestart() throws Exception {
        info("*** STARTING TEST ***");

        startGrids();

        try {
            IgniteCache<Integer, String> c = grid(idx).cache(CACHE_NAME);

            for (int j = 0; j < retries; j++) {
                for (int i = 0; i < keyCnt; i++)
                    c.put(i, Integer.toString(i));

                info("Stored items.");

                checkGet(c, j);

                info("Stopping node: " + idx);

                affinityBeforeStop(c);

                stopGrid(idx);

                info("Starting node: " + idx);

                Ignite ignite = startGrid(idx);

                c = ignite.cache(CACHE_NAME);

                affinityAfterStart(c);

                checkGet(c, j);
            }
        }
        finally {
            stopGrids();
        }
    }

    /**
     * @param c Cache.
     * @param attempt Attempt.
     * @throws Exception If failed.
     */
    private void checkGet(IgniteCache<Integer, String> c, int attempt) throws Exception {
        for (int i = 0; i < keyCnt; i++) {
            String v = c.get(i);

            if (v == null) {
                printFailureDetails(c, i, attempt);

                fail("Value is null [key=" + i + ", attempt=" + attempt + "]");
            }

            if (!Integer.toString(i).equals(v)) {
                printFailureDetails(c, i, attempt);

                fail("Wrong value for key [key=" +
                    i + ", actual value=" + v + ", expected value=" + Integer.toString(i) + "]");
            }
        }

        info("Read items.");
    }

    /**
     * @param c Cache projection.
     * @param key Key.
     * @param attempt Attempt.
     */
    private void printFailureDetails(IgniteCache<Integer, String> c, int key, int attempt) {
        error("*** Failure details ***");
        error("Key: " + key);
        error("Partition: " + c.getConfiguration(CacheConfiguration.class).getAffinity().partition(key));
        error("Attempt: " + attempt);
        error("Node: " + c.unwrap(Ignite.class).cluster().localNode().id());
    }
}