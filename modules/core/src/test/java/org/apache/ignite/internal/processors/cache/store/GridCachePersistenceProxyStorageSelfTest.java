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

package org.apache.ignite.internal.processors.cache.store;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheDataStoreProxy;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;

/**
 *
 */
public class GridCachePersistenceProxyStorageSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /**
     *
     */
    @After
    public void afterProxyTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    public void testCacheDataStoreSwitchingBase() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite0.getOrCreateCache(
            new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false)
                    .setPartitions(4)));

        loadData(ignite0, DEFAULT_CACHE_NAME, 100_000);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ignite0.context().cache().context()
            .database();

        dbMgr.wakeupForCheckpoint("save").get();

        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(ignite0);

        spi0.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return (msg instanceof GridDhtPartitionDemandMessage)
                    && ((GridCacheGroupIdMessage)msg).groupId() == groupIdForCache(ignite0, DEFAULT_CACHE_NAME);
            }
        });

        IgniteEx ignite1 = startGrid(1);

        assertTrue(!ignite0.cluster().isBaselineAutoAdjustEnabled());

        ignite0.cluster().setBaselineTopology(ignite0.cluster().nodes());

        System.out.println("start blocking");

        spi0.waitForBlocked();

        CacheGroupContext grp = ignite1.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

        int partitions = grp.affinity().partitions();

        GridDhtPartitionTopology top = grp.topology();

        for (int partId = 0; partId < partitions; partId++) {
            GridDhtLocalPartition part = top.localPartition(partId);

            assert part.state() == MOVING;

            part.storageMode(CacheDataStoreProxy.StorageMode.LOG_ONLY);
        }

        for (int i = 1024; i < 2048; i++)
            cache.put(i, new byte[2000]);

        stopAllGrids();
    }
}
