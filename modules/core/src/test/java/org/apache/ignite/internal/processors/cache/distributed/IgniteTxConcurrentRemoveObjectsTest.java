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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgniteTxConcurrentRemoveObjectsTest extends GridCommonAbstractTest {
    /** Cache partitions. */
    private static final int CACHE_PARTITIONS = 16;

    /** Cache entries count. */
    private static final int CACHE_ENTRIES_COUNT = 512 * CACHE_PARTITIONS;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** New value for {@link IgniteSystemProperties#IGNITE_CACHE_REMOVED_ENTRIES_TTL} property. */
    private static final long newIgniteCacheRemovedEntriesTtl = 50L;

    /** Old value of {@link IgniteSystemProperties#IGNITE_CACHE_REMOVED_ENTRIES_TTL} property. */
    private static long oldIgniteCacheRmvEntriesTtl;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        oldIgniteCacheRmvEntriesTtl = Long.getLong(IGNITE_CACHE_REMOVED_ENTRIES_TTL, 10_000);

        System.setProperty(IGNITE_CACHE_REMOVED_ENTRIES_TTL, Long.toString(newIgniteCacheRemovedEntriesTtl));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.setProperty(IGNITE_CACHE_REMOVED_ENTRIES_TTL, Long.toString(oldIgniteCacheRmvEntriesTtl));

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, String> getCacheCfg() {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>();

        ccfg.setName(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        ccfg.setAffinity(new RendezvousAffinityFunction().setPartitions(CACHE_PARTITIONS));

        return ccfg;
    }

    /**
     * Too many deletes in single transaction may overflow {@link GridDhtLocalPartition#rmvQueue} and entries will be
     * deleted synchronously in {@link GridDhtLocalPartition#onDeferredDelete(int, KeyCacheObject, GridCacheVersion)}.
     * This should not corrupt internal map state in {@link GridDhtLocalPartition}.
     *
     * @throws Exception If failed.
     */
    public void testTxLeavesObjectsInLocalPartition() throws Exception {
        IgniteEx igniteEx = startGrid(getConfiguration());

        igniteEx.getOrCreateCache(getCacheCfg());

        try (IgniteDataStreamer<Integer, String> dataStreamer = igniteEx.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < CACHE_ENTRIES_COUNT; i++)
                dataStreamer.addData(i, UUID.randomUUID().toString());
        }

        IgniteEx client = startGrid(
            getConfiguration()
                .setClientMode(true)
                .setIgniteInstanceName(UUID.randomUUID().toString())
        );

        awaitPartitionMapExchange();

        assertEquals(CACHE_ENTRIES_COUNT, client.getOrCreateCache(DEFAULT_CACHE_NAME).size());

        try (Transaction tx = client.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
            IgniteCache<Integer, String> cache = client.getOrCreateCache(getCacheCfg());

            for (int v = 0; v < CACHE_ENTRIES_COUNT; v++) {
                cache.get(v);

                cache.remove(v);
            }

            tx.commit();
        }

        GridTestUtils.waitForCondition(
            () -> igniteEx.context().cache().cacheGroups().stream()
                .filter(CacheGroupContext::userCache)
                .flatMap(cgctx -> cgctx.topology().localPartitions().stream())
                .mapToInt(GridDhtLocalPartition::internalSize)
                .max().orElse(-1) == 0,
            newIgniteCacheRemovedEntriesTtl * 10
        );
    }
}
