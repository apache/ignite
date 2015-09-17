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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.Map;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test for issue GG-3997 Total Hits and Misses display wrong value for in-memory database.
 */
public class GridCachePartitionedHitsAndMissesSelfTest extends GridCommonAbstractTest {
    /** Amount of grids to start. */
    private static final int GRID_CNT = 3;

    /** Count of total numbers to generate. */
    private static final int CNT = 2000;

    /** IP Finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // DiscoverySpi.
        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(IP_FINDER);
        cfg.setDiscoverySpi(disco);

        // Cache.
        cfg.setCacheConfiguration(cacheConfiguration());

        TransactionConfiguration tCfg = new TransactionConfiguration();

        tCfg.setDefaultTxConcurrency(PESSIMISTIC);
        tCfg.setDefaultTxIsolation(REPEATABLE_READ);

        cfg.setTransactionConfiguration(tCfg);

        return cfg;
    }

    /**
     * Cache configuration.
     *
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setStartSize(700000);
        cfg.setWriteSynchronizationMode(FULL_ASYNC);
        cfg.setEvictionPolicy(null);
        cfg.setBackups(1);
        cfg.setNearConfiguration(null);
        cfg.setStatisticsEnabled(true);

        return cfg;
    }

    /**
     * This test is just a wrapper for org.apache.ignite.examples.datagrid.CachePopularNumbersExample
     *
     * @throws Exception If failed.
     */
    public void testHitsAndMisses() throws Exception {
        startGrids(GRID_CNT);

        awaitPartitionMapExchange();

        try {
            final Ignite g = grid(0);

            realTimePopulate(g);

            // Check metrics for the whole cache.
            long hits = 0;
            long misses = 0;

            for (int i = 0; i < GRID_CNT; i++) {
                CacheMetrics m = grid(i).cache(null).metrics();

                hits += m.getCacheHits();
                misses += m.getCacheMisses();
            }

            // Check that invoke and loader updated metrics
            assertEquals(CNT, hits);
            assertEquals(CNT, misses);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Populates cache with data streamer.
     *
     * @param g Grid.
     */
    private static void realTimePopulate(final Ignite g) {
        try (IgniteDataStreamer<Integer, Long> ldr = g.dataStreamer(null)) {
            // Sets max values to 1 so cache metrics have correct values.
            ldr.perNodeParallelOperations(1);

            // Count closure which increments a count on remote node.
            ldr.receiver(new IncrementingUpdater());

            for (int i = 0; i < CNT; i++)
                ldr.addData(i % (CNT / 2), 1L);
        }
    }

    /**
     * Increments value for key.
     */
    private static class IncrementingUpdater implements StreamReceiver<Integer, Long> {
        /** */
        private static final EntryProcessor<Integer, Long, Void> INC = new EntryProcessor<Integer, Long, Void>() {
            @Override public Void process(MutableEntry<Integer, Long> e, Object... args) {
                Long val = e.getValue();

                e.setValue(val == null ? 1 : val + 1);

                return null;
            }
        };

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache<Integer, Long> cache, Collection<Map.Entry<Integer, Long>> entries) {
            for (Map.Entry<Integer, Long> entry : entries)
                cache.invoke(entry.getKey(), INC);
        }
    }
}