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

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheDataStoreProxy;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

/**
 *
 */
public class GridCachePersistenceProxyStorageSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
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
                .setAffinity(new RendezvousAffinityFunction(false)
                    .setPartitions(1)));

        CountDownLatch latch = new CountDownLatch(1);

        GridTestUtils.runMultiThreadedAsync(() -> {
                try {
                    latch.await();

                    for (int i = 0; i < 1024; i++)
                        cache.put(i, new byte[2000]);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            },
            2,
            "cache-async-putter");

        CacheGroupContext grp = ignite0.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

        int partitions = grp.affinity().partitions();

        GridDhtPartitionTopology top = grp.topology();

        latch.countDown();

        for (int partId = 0; partId < partitions; partId++) {
            GridDhtLocalPartition part = top.localPartition(partId);

            part.storageMode(CacheDataStoreProxy.StorageMode.LOG_ONLY);
        }

        for (int i = 1024; i < 2048; i++)
            cache.put(i, new byte[2000]);

        stopGrid(0);
    }
}
