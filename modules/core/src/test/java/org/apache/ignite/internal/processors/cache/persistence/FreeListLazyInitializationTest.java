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

import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 *
 */
public class FreeListLazyInitializationTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024).setPersistenceEnabled(true))
            .setPageSize(1024)
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false));

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        CacheConfiguration ccfg1 = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg1.setWriteSynchronizationMode(FULL_SYNC);
        ccfg1.setAtomicityMode(TRANSACTIONAL);
        ccfg1.setCacheMode(REPLICATED);

        CacheConfiguration ccfg2 = new CacheConfiguration("cache-2");
        ccfg2.setWriteSynchronizationMode(FULL_SYNC);
        ccfg2.setAtomicityMode(TRANSACTIONAL);
        ccfg2.setCacheMode(PARTITIONED);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLazyFreeListInitialization() throws Exception {
        IgniteEx node = startGrid(0);

        node.active(true);

        int parts = node.affinity(DEFAULT_CACHE_NAME).partitions();

        checkLazyFreeList(node, parts, false);

        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < parts; i++)
            map.put(i, i);

        node.cache(DEFAULT_CACHE_NAME).putAll(map);
        node.cache(DEFAULT_CACHE_NAME).removeAll(map.keySet());

        forceCheckpoint(node);

        checkLazyFreeList(node, parts, true);

        stopAllGrids();

        node = startGrid(0);
        node.active(true);

        checkLazyFreeList(node, parts, false);

        // Check second cache and checkpoints do not force unnecessary free lists initialization.
        node.cache("cache-2").putAll(map);
        node.cache("cache-2").removeAll(map.keySet());

        forceCheckpoint(node);

        checkLazyFreeList(node, parts, false);

        node.cache(DEFAULT_CACHE_NAME).putAll(map);

        checkLazyFreeList(node, parts, true);
    }

    /**
     * @param node Node.
     * @param expParts Expected partitions number.
     * @param expInit {@code True} if free lists should be initialized.
     */
    private void checkLazyFreeList(IgniteEx node, int expParts, boolean expInit) {
        GridCacheContext ctx = node.context().cache().cache(DEFAULT_CACHE_NAME).context();

        List<GridDhtLocalPartition> partsList = ctx.topology().localPartitions();

        assertEquals(expParts, partsList.size());

        for (GridDhtLocalPartition part : partsList) {
            CacheFreeList freeList = getFieldValue(part.dataStore(), "freeList");

            if (expInit || freeList != null)
                assertTrue(freeList.getClass().getName(), freeList instanceof LazyCacheFreeList);

            Object delegate = freeList != null ? getFieldValue(freeList, LazyCacheFreeList.class, "delegate") : null;

            if (expInit)
                assertNotNull(delegate);
            else
                assertNull(delegate);
        }
    }
}
