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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheMvccClusterRestartTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setWalMode(WALMode.LOG_ONLY);
        storageCfg.setPageSize(1024);

        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        regionCfg.setPersistenceEnabled(true);
        regionCfg.setMaxSize(100 * 1024 * 1024);

        storageCfg.setDefaultDataRegionConfiguration(regionCfg);

        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9394");

        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestart1() throws Exception {
       restart1(3, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestart2() throws Exception {
        restart1(1, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestart3() throws Exception {
        restart1(3, 1);
    }

    /**
     * @param srvBefore Number of servers before restart.
     * @param srvAfter Number of servers after restart.
     * @throws Exception If failed.
     */
    private void restart1(int srvBefore, int srvAfter) throws Exception {
        Ignite srv0 = startGridsMultiThreaded(srvBefore);

        IgniteCache<Object, Object> cache = srv0.createCache(cacheConfiguration());

        Set<Integer> keys = new HashSet<>(primaryKeys(cache, 1, 0));

        try (Transaction tx = srv0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (Integer k : keys)
                cache.put(k, k);

            tx.commit();
        }

        stopAllGrids();

        srv0 = startGridsMultiThreaded(srvAfter);

        cache = srv0.cache(DEFAULT_CACHE_NAME);

        Map<Object, Object> res = cache.getAll(keys);

        assertEquals(keys.size(), res.size());

        for (Integer k : keys)
            assertEquals(k, cache.get(k));

        try (Transaction tx = srv0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (Integer k : keys)
                cache.put(k, k + 1);

            tx.commit();
        }

        for (Integer k : keys)
            assertEquals(k + 1, cache.get(k));
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration() {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
        ccfg.setBackups(2);

        return ccfg;
    }
}
