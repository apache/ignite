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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests for {@link GridCacheMvccManager}.
 */
public class GridCacheMvccManagerSelfTest extends GridCommonAbstractTest {
    /** VM ip finder for TCP discovery. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Cache mode. */
    private CacheMode mode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);
        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);
        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /** @return Cache configuration. */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(mode);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setAtomicityMode(TRANSACTIONAL);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testLocalCache() throws Exception {
        mode = LOCAL;

        testCandidates(1);
    }

    /** @throws Exception If failed. */
    public void testReplicatedCache() throws Exception {
        mode = REPLICATED;

        testCandidates(3);
    }

    /** @throws Exception If failed. */
    public void testPartitionedCache() throws Exception {
        mode = PARTITIONED;

        testCandidates(3);
    }

    /**
     * @param gridCnt Grid count.
     * @throws Exception If failed.
     */
    private void testCandidates(int gridCnt) throws Exception {
        try {
            Ignite ignite = startGridsMultiThreaded(gridCnt);

            IgniteCache<Integer, Integer> cache = ignite.cache(null);

            Transaction tx = ignite.transactions().txStart();

            cache.put(1, 1);

            tx.commit();

            for (int i = 0; i < gridCnt; i++) {
                assert ((IgniteKernal)grid(i)).internalCache().context().mvcc().localCandidates().isEmpty();
                assert ((IgniteKernal)grid(i)).internalCache().context().mvcc().remoteCandidates().isEmpty();
            }
        }
        finally {
            stopAllGrids();
        }
    }
}