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

package org.apache.ignite.cache.affinity;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class AffinityDistributionPrintLogTest extends GridCommonAbstractTest {
    /** */
    private int parts = 1024;

    /** */
    private final String CHECK_MESSAGE = "Local node affinity assignment distribution is not ideal";

    /** */
    private int backups = 2;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setIdleConnectionTimeout(100);

        cfg.setCommunicationSpi(commSpi);

        MemoryEventStorageSpi evtSpi = new MemoryEventStorageSpi();
        evtSpi.setExpireCount(50);

        cfg.setEventStorageSpi(evtSpi);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setNearConfiguration(null);

        RendezvousAffinityFunction aff = new RendezvousAffinityFunction();
        aff.setPartitions(parts);

        ccfg.setAffinity(aff);

        cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionCalculationForThreePartitionsNotIdealMessage() throws Exception {
        parts = 3;

        String log = print(true, 0.0, 1);

        assertTrue(log.contains(CHECK_MESSAGE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionCalculationNotIdealMessage() throws Exception {
        String log = print(false, 0.01,2);

        assertTrue(log.contains(CHECK_MESSAGE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionCalculationIdeal() throws Exception {
        String log = print(false, 0.5, 2);

        assertFalse(log.contains(CHECK_MESSAGE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionCalculationDefault() throws Exception {
        String log = print(true, 0, 2);

        assertFalse(log.contains(CHECK_MESSAGE));
    }

    /**
     * @param useDfltPartDistributionThreshold Use default IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD.
     * @param partDistributionThreshold Value of IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD for setting.
     * @param nodeCnt Node count.
     * @return Intercepted log.
     * @throws Exception If failed.
     */
    public String print(boolean useDfltPartDistributionThreshold, double partDistributionThreshold, int nodeCnt) throws Exception {
        if (!useDfltPartDistributionThreshold)
            System.setProperty(IgniteSystemProperties.IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, String.valueOf(partDistributionThreshold));

        Ignite ignite = startGrids(nodeCnt);

        awaitPartitionMapExchange();

        final GridStringLogger log = new GridStringLogger(false, this.log);

        GridCacheProcessor proc = ((IgniteKernal)ignite).context().cache();

        for (GridCacheContext cctx : proc.context().cacheContexts()) {
            GridAffinityAssignmentCache aff = GridTestUtils.getFieldValue(cctx.affinity(), "aff");

            GridTestUtils.setFieldValue(aff, "log", log);
        }

        startGrid(2);

        awaitPartitionMapExchange();

        return log.toString();
    }
}
