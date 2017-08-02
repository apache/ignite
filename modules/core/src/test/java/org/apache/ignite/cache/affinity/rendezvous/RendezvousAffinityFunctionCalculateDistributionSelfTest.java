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

package org.apache.ignite.cache.affinity.rendezvous;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
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
 * Tests for {@link RendezvousAffinityFunction}.
 */
public class RendezvousAffinityFunctionCalculateDistributionSelfTest extends GridCommonAbstractTest {
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    private int backups = 3;

    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

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

        cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionCalculationOkMessage() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, String.valueOf(10));

        File file = new File(home() + "/work/log/ignite.log");

        new PrintWriter(file).close();

        Ignite ignite = startGridsMultiThreaded(2);

        awaitPartitionMapExchange();

        ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        Affinity<Object> aff = ignite.affinity(DEFAULT_CACHE_NAME);

        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(ignite.cluster().nodes()), null, null,
                new AffinityTopologyVersion(2), backups);

//        affinityFunction().assignPartitions(ctx);

//        assertTrue(FileUtils.readFileToString(file).contains("Partition map has been built (distribution is not even for caches) [cacheName=ignite-atomics-sys-cache, Primary nodeId="
//            + grid(1).configuration().getNodeId() + ", totalPartitionsCount=1024 percentageOfTotalPartsCount=51%, parts=524]"));
//
//        assertTrue(FileUtils.readFileToString(file).contains("Partition map has been built (distribution is not even for caches) [cacheName=ignite-atomics-sys-cache, Primary nodeId="
//            + grid(0).configuration().getNodeId() + ", totalPartitionsCount=1024 percentageOfTotalPartsCount=49%, parts=500]"));

        String res = log.toString();

        assertTrue(res.contains("/* PUBLIC.RANGE_INDEX */"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionCalculationProblemMessage() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, String.valueOf(55));

        File file = new File(home() + "/work/log/ignite.log");

        new PrintWriter(file).close();

        Ignite ignite = startGrids(2);

        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(ignite.cluster().nodes()), null, null,
                new AffinityTopologyVersion(1), 1);

//        affinityFunction().assignPartitions(ctx);

        assertTrue(FileUtils.readFileToString(file).contains("Partition map has been built (distribution is even)"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionCalculationDisable() throws Exception {
        Ignite ignite = startGrid(0);

        awaitPartitionMapExchange();

        final GridStringLogger log = new GridStringLogger(false, this.log);

        GridCacheProcessor proc = ((IgniteKernal)ignite).context().cache();

        for (GridCacheContext cctx : proc.context().cacheContexts()) {
            GridAffinityAssignmentCache aff = GridTestUtils.getFieldValue(cctx.affinity(), "aff");

            GridTestUtils.setFieldValue(aff, "log", log);
        }

        startGrid(1);

        String res = log.toString();

        assertFalse(res.contains("Partition map has been built (distribution is not even for caches)"));
        assertFalse(res.contains("Partition map has been built (distribution is even)"));
    }
}
