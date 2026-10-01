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

import java.util.Collection;
import java.util.LinkedList;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_REBALANCE_BATCH_SIZE;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 */
public class GridCacheDhtPreloadStartStopSelfTest extends GridCommonAbstractTest {
    /** */
    private static final long TEST_TIMEOUT = 5 * 60 * 1000;

    /** Default backups. */
    private static final int DFLT_BACKUPS = 1;

    /** Partitions. */
    private static final int DFLT_PARTITIONS = 521;

    /** Preload batch size. */
    private static final int DFLT_BATCH_SIZE = DFLT_REBALANCE_BATCH_SIZE;

    /** Default cache count. */
    private static final int DFLT_CACHE_CNT = 10;

    /** Number of key backups. Each test method can set this value as required. */
    private int backups = DFLT_BACKUPS;

    /** Preload mode. */
    private CacheRebalanceMode preloadMode = ASYNC;

    /** */
    private int preloadBatchSize = DFLT_BATCH_SIZE;

    /** Number of partitions. */
    private int partitions = DFLT_PARTITIONS;

    /** */
    private int cacheCnt = DFLT_CACHE_CNT;

    /**
     *
     */
    public GridCacheDhtPreloadStartStopSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration[] cacheCfgs = new CacheConfiguration[cacheCnt];

        for (int i = 0; i < cacheCnt; i++) {
            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setName("partitioned-" + i);

            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setRebalanceBatchSize(preloadBatchSize);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg.setRebalanceMode(preloadMode);
            cacheCfg.setAffinity(new RendezvousAffinityFunction(false, partitions));
            cacheCfg.setBackups(backups);
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            cacheCfgs[i] = cacheCfg;
        }

        cfg.setCacheConfiguration(cacheCfgs);
        cfg.setDeploymentMode(CONTINUOUS);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
        preloadMode = ASYNC;
        preloadBatchSize = DFLT_BATCH_SIZE;
        cacheCnt = DFLT_CACHE_CNT;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * @param cnt Number of grids.
     * @param startIdx Start node index.
     * @param list List of started grids.
     * @throws Exception If failed.
     */
    private void startGrids(int cnt, int startIdx, Collection<Ignite> list) throws Exception {
        for (int i = 0; i < cnt; i++)
            list.add(startGrid(startIdx++));
    }

    /** @param grids Grids to stop. */
    private void stopGrids(Iterable<Ignite> grids) {
        for (Ignite g : grids)
            stopGrid(g.name());
    }

    /** @throws Exception If failed. */
    @Test
    public void testDeadlock() throws Exception {
        info("Testing deadlock...");

        Collection<Ignite> ignites = new LinkedList<>();

        int gridCnt = 3;

        startGrids(gridCnt, 1, ignites);

        info("Grids started: " + gridCnt);

        stopGrids(ignites);
    }
}
