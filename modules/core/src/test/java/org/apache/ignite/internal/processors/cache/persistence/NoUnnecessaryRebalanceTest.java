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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.util.TestStorageUtils.corruptDataEntry;

/**
 * Tests check that unnecessary rebalance doesn't happen.
 */
public class NoUnnecessaryRebalanceTest extends GridCommonAbstractTest {
    /** Cache configuration name prefix. */
    private static final String CACHE_NAME = "testCache";

    /** Number of cluster nodes. */
    private static final int NODE_COUNT = 3;

    /**
     * @return Grid test configuration.
     * @throws Exception If failed.
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /**
     * Test check that cache creation doesn't invoke rebalance on cache in other cache group.
     * @throws Exception If failed.
     */
    @Test
    public void testNoRebalanceOnCacheCreation() throws Exception {
        Ignite g0 = startGrids(NODE_COUNT);

        g0.cluster().state(ClusterState.ACTIVE);
        g0.createCache(getCacheConfiguration(0));

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache0 = g0.cache(CACHE_NAME + 0);

        for (int i = 0; i < 100; i++)
            cache0.put(i, i);

        awaitPartitionMapExchange();

        GridCacheContext<Object, Object> cacheCtx0 = grid(0).cachex(CACHE_NAME + 0).context();

        corruptDataEntry(cacheCtx0, 1, true, false);

        G.allGrids().forEach(n -> TestRecordingCommunicationSpi.spi(n)
            .record((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage));

        g0.createCache(getCacheConfiguration(1));

        awaitPartitionMapExchange(true, true, null);

        List<Object> msgs = new ArrayList<>();

        G.allGrids().forEach(n -> msgs.addAll(TestRecordingCommunicationSpi.spi(n).recordedMessages(true)));

        assertFalse(msgs.stream().map(o -> ((GridCacheGroupIdMessage)o).groupId()).collect(Collectors.toSet())
            .contains(CU.cacheId(CACHE_NAME + 0)));
    }

    /**
     * @param postFix Cache name postfix.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> getCacheConfiguration(int postFix) {
        return new CacheConfiguration<>(CACHE_NAME + postFix).setBackups(2);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }
}
