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

import java.io.File;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;

/** */
@RunWith(JUnit4.class)
public class GridCacheRebalancingPartitionUploadTest extends GridCommonAbstractTest {
    /** */
    private static final String DHT_PARTITIONED_CACHE = "cacheP";

    private static final int CACHE_PARTITIONS = 8;

    /** */
    @Before
    public void beforeTestCleanup() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void afterTestCleanup() throws Exception {
        stopAllGrids();

//        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(true)
            )
            .setPageSize(1024)
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     * @throws Exception Exception.
     */
    @Test
    public void testClientNodeJoinAtRebalancing() throws Exception {
        final IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite0.createCache(
            new CacheConfiguration<Integer, Integer>(DHT_PARTITIONED_CACHE)
                .setCacheMode(CacheMode.PARTITIONED)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setBackups(1)
                .setRebalanceOrder(2)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setAffinity(new RendezvousAffinityFunction(false)
                    .setPartitions(CACHE_PARTITIONS)));

        for (int i = 0; i < 2048; i++)
            cache.put(i, i);

        IgniteEx ignite1 = startGrid(1);

        ignite1.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

        awaitPartitionMapExchange();

        // Checking downloaded partitions.
        final CacheGroupContext grpCtx = ignite1.context().cache().cacheGroup(CU.cacheId(DHT_PARTITIONED_CACHE));

        File grpDir = cacheWorkDir(((GridCacheDatabaseSharedManager)grpCtx.shared().database())
                .getFileStoreManager()
                .workDir(),
            grpCtx.config());

        int countParts = new File(grpDir, "downloads").list().length;

        assertEquals(CACHE_PARTITIONS, countParts);
    }
}
