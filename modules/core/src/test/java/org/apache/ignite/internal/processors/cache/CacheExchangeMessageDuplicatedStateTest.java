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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheExchangeMessageDuplicatedStateTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String AFF1_CACHE1 = "a1c1";

    /** */
    private static final String AFF1_CACHE2 = "a1c2";

    /** */
    private static final String AFF2_CACHE1 = "a2c1";

    /** */
    private static final String AFF2_CACHE2 = "a2c2";

    /** */
    private static final String AFF3_CACHE1 = "a3c1";

    /** */
    private static final String AFF4_FILTER_CACHE1 = "a4c1";

    /** */
    private static final String AFF4_FILTER_CACHE2 = "a4c2";

    /** */
    private static final String AFF5_FILTER_CACHE1 = "a5c1";

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        commSpi.record(GridDhtPartitionsSingleMessage.class, GridDhtPartitionsFullMessage.class);

        cfg.setCommunicationSpi(commSpi);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        {
            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setName(AFF1_CACHE1);
            ccfg.setAffinity(new RendezvousAffinityFunction());
            ccfgs.add(ccfg);
        }
        {
            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setName(AFF1_CACHE2);
            ccfg.setAffinity(new RendezvousAffinityFunction());
            ccfgs.add(ccfg);
        }
        {
            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setName(AFF2_CACHE1);
            ccfg.setAffinity(new FairAffinityFunction());
            ccfgs.add(ccfg);
        }
        {
            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setName(AFF2_CACHE2);
            ccfg.setAffinity(new FairAffinityFunction());
            ccfgs.add(ccfg);
        }
        {
            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setName(AFF3_CACHE1);
            ccfg.setBackups(3);

            RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false, 64);
            ccfg.setAffinity(aff);

            ccfgs.add(ccfg);
        }
        {
            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setName(AFF4_FILTER_CACHE1);
            ccfg.setNodeFilter(new TestNodeFilter());
            ccfg.setAffinity(new RendezvousAffinityFunction());
            ccfgs.add(ccfg);
        }
        {
            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setName(AFF4_FILTER_CACHE2);
            ccfg.setNodeFilter(new TestNodeFilter());
            ccfg.setAffinity(new RendezvousAffinityFunction());
            ccfgs.add(ccfg);
        }
        {
            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setName(AFF5_FILTER_CACHE1);
            ccfg.setNodeFilter(new TestNodeFilter());
            ccfg.setAffinity(new FairAffinityFunction());
            ccfgs.add(ccfg);
        }

        cfg.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testExchangeMessages() throws Exception {
        ignite(0);

        startGrid(1);

        awaitPartitionMapExchange();

        checkMessages(0, true);

        startGrid(2);

        awaitPartitionMapExchange();

        checkMessages(0, true);

        client = true;

        startGrid(3);

        awaitPartitionMapExchange();

        checkMessages(0, false);

        stopGrid(0);

        awaitPartitionMapExchange();

        checkMessages(1, true);
    }

    /**
     * @param crdIdx Coordinator node index.
     * @param checkSingle {@code True} if need check single messages.
     */
    private void checkMessages(int crdIdx, boolean checkSingle) {
        checkFullMessages(crdIdx);

        if (checkSingle)
            checkSingleMessages(crdIdx);
    }

    /**
     * @param crdIdx Coordinator node index.
     */
    private void checkFullMessages(int crdIdx) {
        TestRecordingCommunicationSpi commSpi0 =
            (TestRecordingCommunicationSpi)ignite(crdIdx).configuration().getCommunicationSpi();

        List<Object> msgs = commSpi0.recordedMessages(false);

        assertTrue(msgs.size() > 0);

        for (Object msg : msgs) {
            assertTrue("Unexpected messages: " + msg, msg instanceof GridDhtPartitionsFullMessage);

            checkFullMessage((GridDhtPartitionsFullMessage)msg);
        }
    }

    /**
     * @param crdIdx Coordinator node index.
     */
    private void checkSingleMessages(int crdIdx) {
        int cnt = 0;

        for (Ignite ignite : Ignition.allGrids()) {
            if (getTestGridName(crdIdx).equals(ignite.name()) || ignite.configuration().isClientMode())
                continue;

            TestRecordingCommunicationSpi commSpi0 =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            List<Object> msgs = commSpi0.recordedMessages(false);

            assertTrue(msgs.size() > 0);

            for (Object msg : msgs) {
                assertTrue("Unexpected messages: " + msg, msg instanceof GridDhtPartitionsSingleMessage);

                checkSingleMessage((GridDhtPartitionsSingleMessage)msg);
            }

            cnt++;
        }

        assertTrue(cnt > 0);
    }

    /**
     * @param msg Message.
     */
    private void checkFullMessage(GridDhtPartitionsFullMessage msg) {
        Map<Integer, Integer> dupPartsData = GridTestUtils.getFieldValue(msg, "dupPartsData");

        assertNotNull(dupPartsData);

        checkFullMessage(AFF1_CACHE1, AFF1_CACHE2, dupPartsData, msg);
        checkFullMessage(AFF2_CACHE1, AFF2_CACHE2, dupPartsData, msg);
        checkFullMessage(AFF4_FILTER_CACHE1, AFF4_FILTER_CACHE2, dupPartsData, msg);

        assertFalse(dupPartsData.containsKey(CU.cacheId(AFF3_CACHE1)));
        assertFalse(dupPartsData.containsKey(CU.cacheId(AFF5_FILTER_CACHE1)));

        Map<Integer, Map<Integer, Long>> partCntrs = GridTestUtils.getFieldValue(msg, "partCntrs");

        if (partCntrs != null) {
            for (Map<Integer, Long> cntrs : partCntrs.values())
                assertTrue(cntrs.isEmpty());
        }
    }

    /**
     * @param msg Message.
     */
    private void checkSingleMessage(GridDhtPartitionsSingleMessage msg) {
        Map<Integer, Integer> dupPartsData = GridTestUtils.getFieldValue(msg, "dupPartsData");

        assertNotNull(dupPartsData);

        checkSingleMessage(AFF1_CACHE1, AFF1_CACHE2, dupPartsData, msg);
        checkSingleMessage(AFF2_CACHE1, AFF2_CACHE2, dupPartsData, msg);
        checkSingleMessage(AFF4_FILTER_CACHE1, AFF4_FILTER_CACHE2, dupPartsData, msg);

        assertFalse(dupPartsData.containsKey(CU.cacheId(AFF3_CACHE1)));
        assertFalse(dupPartsData.containsKey(CU.cacheId(AFF5_FILTER_CACHE1)));

        Map<Integer, Map<Integer, Long>> partCntrs = GridTestUtils.getFieldValue(msg, "partCntrs");

        if (partCntrs != null) {
            for (Map<Integer, Long> cntrs : partCntrs.values())
                assertTrue(cntrs.isEmpty());
        }
    }

    /**
     * @param cache1 Cache 1.
     * @param cache2 Cache 2.
     * @param dupPartsData Duplicated data map.
     * @param msg Message.
     */
    private void checkFullMessage(String cache1,
        String cache2,
        Map<Integer, Integer> dupPartsData,
        GridDhtPartitionsFullMessage msg)
    {
        Integer cacheId;
        Integer dupCacheId;

        if (dupPartsData.containsKey(CU.cacheId(cache1))) {
            cacheId = CU.cacheId(cache1);
            dupCacheId = CU.cacheId(cache2);
        }
        else {
            cacheId = CU.cacheId(cache2);
            dupCacheId = CU.cacheId(cache1);
        }

        assertTrue(dupPartsData.containsKey(cacheId));
        assertEquals(dupCacheId, dupPartsData.get(cacheId));
        assertFalse(dupPartsData.containsKey(dupCacheId));

        Map<Integer, GridDhtPartitionFullMap> parts = msg.partitions();

        GridDhtPartitionFullMap emptyFullMap = parts.get(cacheId);

        for (GridDhtPartitionMap2 map : emptyFullMap.values())
            assertEquals(0, map.map().size());

        GridDhtPartitionFullMap fullMap = parts.get(dupCacheId);

        for (GridDhtPartitionMap2 map : fullMap.values())
            assertFalse(map.map().isEmpty());
    }

    /**
     * @param cache1 Cache 1.
     * @param cache2 Cache 2.
     * @param dupPartsData Duplicated data map.
     * @param msg Message.
     */
    private void checkSingleMessage(String cache1,
        String cache2,
        Map<Integer, Integer> dupPartsData,
        GridDhtPartitionsSingleMessage msg)
    {
        Integer cacheId;
        Integer dupCacheId;

        if (dupPartsData.containsKey(CU.cacheId(cache1))) {
            cacheId = CU.cacheId(cache1);
            dupCacheId = CU.cacheId(cache2);
        }
        else {
            cacheId = CU.cacheId(cache2);
            dupCacheId = CU.cacheId(cache1);
        }

        assertTrue(dupPartsData.containsKey(cacheId));
        assertEquals(dupCacheId, dupPartsData.get(cacheId));
        assertFalse(dupPartsData.containsKey(dupCacheId));

        Map<Integer, GridDhtPartitionMap2> parts = msg.partitions();

        GridDhtPartitionMap2 emptyMap = parts.get(cacheId);

        assertEquals(0, emptyMap.map().size());

        GridDhtPartitionMap2 map = parts.get(dupCacheId);

        assertFalse(map.map().isEmpty());
    }

    /**
     *
     */
    private static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            // Do not start cache on coordinator.
            return node.order() > 1;
        }
    }
}
