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
package org.apache.ignite.internal.processors.cache.persistence.db.file;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class DefaultPageSizeBackwardsCompatibilityTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Client mode. */
    private boolean set2kPageSize = true;

    /** Entries count. */
    public static final int ENTRIES_COUNT = 300;

    /** Cache name. */
    public static final String CACHE_NAME = "cache1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoverySpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoverySpi.setIpFinder(IP_FINDER);

        MemoryConfiguration memCfg = new MemoryConfiguration();

        if (set2kPageSize)
            memCfg.setPageSize(2048);

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();
        memPlcCfg.setMaxSize(100 * 1000 * 1000);

        memPlcCfg.setName("dfltMemPlc");

        memCfg.setMemoryPolicies(memPlcCfg);
        memCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(memCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName(CACHE_NAME);
        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg1);

        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration().setCheckpointingFrequency(3_000));

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartFrom2kDefaultStore() throws Exception {
        startGrids(2);

        Ignite ig = ignite(0);

        ig.active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache = ig.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < ENTRIES_COUNT; i++)
            cache.put(i, i);

        Thread.sleep(5_000); // Await for checkpoint to happen.

        stopAllGrids();

        set2kPageSize = false;

        startGrids(2);

        ig = ignite(0);

        ig.active(true);

        awaitPartitionMapExchange();

        cache = ig.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < ENTRIES_COUNT; i++)
            assertEquals((Integer)i, cache.get(i));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }
}
