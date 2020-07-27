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

import java.util.Arrays;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test check that on leave baseline node's partitions marked as lost on mixed region grid.
 */
public class IgniteLostPartitionsOnLeaveBaselineSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(10 * 1024 * 1024)
                .setPersistenceEnabled(true)
                .setMetricsEnabled(true)
                .setName("dflt-plc"))
            .setDataRegionConfigurations(new DataRegionConfiguration()
                .setMaxSize(10 * 1024 * 1024)
                .setPersistenceEnabled(false)
                .setMetricsEnabled(true)
                .setName("no-persistence"))
            .setWalMode(WALMode.LOG_ONLY)
            .setPageSize(4 * 1024)
            .setMetricsEnabled(true);

        cfg.setDataStorageConfiguration(memCfg);

        ((TcpDiscoverySpi) cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @param name          Cache name.
     * @param cacheMode     Cache mode.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(
        String name,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        String dataRegName
    ) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(0);
        ccfg.setCacheMode(cacheMode);
        ccfg.setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);
        ccfg.setDataRegionName(dataRegName);

        return ccfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testLostPartitionsOnLeaveBaseline() throws Exception {
        try {
            final IgniteEx gridFirst = startGrid(0);
            startGrid(1);

            gridFirst.cluster().active(true);

            gridFirst.getOrCreateCaches(Arrays.asList(
                cacheConfiguration("cache-no-persistence", PARTITIONED, ATOMIC, "no-persistence"),
                cacheConfiguration("cache-persistence", PARTITIONED, ATOMIC, null)
            ));

            IgniteInternalCache<Object, Object> cacheNoPersistence = gridFirst.cachex("cache-no-persistence");
            IgniteInternalCache<Object, Object> cachePersistence = gridFirst.cachex("cache-persistence");

            for (int i = 0; i < 10; i++) {
                cacheNoPersistence.put(i, i);
                cachePersistence.put(i, i);
            }

            stopGrid(1);

            resetBaselineTopology();

            assertTrue("List of lost partitions for cache without persistence should not be empty.",
                !cacheNoPersistence.context().topology().lostPartitions().isEmpty());

            assertTrue("List of lost partitions for cache with persistence should not be empty.",
                !cachePersistence.context().topology().lostPartitions().isEmpty());
        } finally {
            stopAllGrids();
        }
    }
}
