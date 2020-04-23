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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionFullCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 *
 */
public class CacheExchangeMessageDuplicatedStateTest extends GridCommonAbstractTest {
    /** */
    private static final String AFF1_CACHE1 = "a1c1";

    /** */
    private static final String AFF1_CACHE2 = "a1c2";

    /** */
    private static final String AFF3_CACHE1 = "a3c1";

    /** */
    private static final String AFF4_FILTER_CACHE1 = "a4c1";

    /** */
    private static final String AFF4_FILTER_CACHE2 = "a4c2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setUserAttributes(Collections.singletonMap("name", igniteInstanceName));

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        commSpi.record(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return (msg.getClass() == GridDhtPartitionsSingleMessage.class ||
                    msg.getClass() == GridDhtPartitionsFullMessage.class) &&
                    ((GridDhtPartitionsAbstractMessage)msg).exchangeId() != null;

            }
        });

        cfg.setCommunicationSpi(commSpi);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
            ccfg.setName(AFF1_CACHE1);
            ccfg.setAffinity(new RendezvousAffinityFunction(false, 512));
            ccfgs.add(ccfg);
        }
        {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
            ccfg.setName(AFF1_CACHE2);
            ccfg.setAffinity(new RendezvousAffinityFunction(false, 512));
            ccfgs.add(ccfg);
        }
        {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
            ccfg.setName(AFF3_CACHE1);
            ccfg.setBackups(3);

            RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false, 64);
            ccfg.setAffinity(aff);

            ccfgs.add(ccfg);
        }
        {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
            ccfg.setName(AFF4_FILTER_CACHE1);
            ccfg.setNodeFilter(new TestNodeFilter());
            ccfg.setAffinity(new RendezvousAffinityFunction());
            ccfgs.add(ccfg);
        }
        {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
            ccfg.setName(AFF4_FILTER_CACHE2);
            ccfg.setNodeFilter(new TestNodeFilter());
            ccfg.setAffinity(new RendezvousAffinityFunction());
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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExchangeMessages() throws Exception {
        ignite(0);

        final int SRVS = 4;

        for (int i = 1; i < SRVS; i++) {
            startGrid(i);

            awaitPartitionMapExchange();

            checkMessages(0, true);
        }

        startClientGrid(SRVS);

        awaitPartitionMapExchange();

        checkMessages(0, false);
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

        assertTrue(!msgs.isEmpty());

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
            if (getTestIgniteInstanceName(crdIdx).equals(ignite.name()) || ignite.configuration().isClientMode())
                continue;

            TestRecordingCommunicationSpi commSpi0 =
                (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();

            List<Object> msgs = commSpi0.recordedMessages(false);

            assertTrue(!msgs.isEmpty());

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
        Map<Integer, Integer> dupPartsData = getFieldValue(msg, "dupPartsData");

        assertNotNull(dupPartsData);

        checkFullMessage(AFF1_CACHE1, AFF1_CACHE2, dupPartsData, msg);
        checkFullMessage(AFF4_FILTER_CACHE1, AFF4_FILTER_CACHE2, dupPartsData, msg);

        assertFalse(dupPartsData.containsKey(CU.cacheId(AFF3_CACHE1)));

        Map<Integer, CachePartitionFullCountersMap> partCntrs =
            getFieldValue(getFieldValue(msg, "partCntrs2"), "map");

        if (partCntrs != null) {
            for (CachePartitionFullCountersMap cntrs : partCntrs.values()) {
                long[] initialUpdCntrs = getFieldValue(cntrs, "initialUpdCntrs");
                long[] updCntrs = getFieldValue(cntrs, "updCntrs");

                for (int i = 0; i < initialUpdCntrs.length; i++) {
                    assertEquals(0, initialUpdCntrs[i]);
                    assertEquals(0, updCntrs[i]);
                }
            }
        }
    }

    /**
     * @param msg Message.
     */
    private void checkSingleMessage(GridDhtPartitionsSingleMessage msg) {
        Map<Integer, Integer> dupPartsData = getFieldValue(msg, "dupPartsData");

        assertNotNull(dupPartsData);

        checkSingleMessage(AFF1_CACHE1, AFF1_CACHE2, dupPartsData, msg);
        checkSingleMessage(AFF4_FILTER_CACHE1, AFF4_FILTER_CACHE2, dupPartsData, msg);

        assertFalse(dupPartsData.containsKey(CU.cacheId(AFF3_CACHE1)));

        Map<Integer, CachePartitionPartialCountersMap> partCntrs = getFieldValue(msg, "partCntrs");

        if (partCntrs != null) {
            for (CachePartitionPartialCountersMap cntrs : partCntrs.values())
                assertEquals(0, cntrs.size());
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
        int cache1Grp = groupIdForCache(ignite(0), cache1);
        int cache2Grp = groupIdForCache(ignite(0), cache2);

        Integer grpId;
        Integer dupGrpId;

        if (dupPartsData.containsKey(cache1Grp)) {
            grpId = cache1Grp;
            dupGrpId = cache2Grp;
        }
        else {
            grpId = cache2Grp;
            dupGrpId = cache1Grp;
        }

        assertTrue(dupPartsData.containsKey(grpId));
        assertEquals(dupGrpId, dupPartsData.get(grpId));
        assertFalse(dupPartsData.containsKey(dupGrpId));

        Map<Integer, GridDhtPartitionFullMap> parts = msg.partitions();

        GridDhtPartitionFullMap emptyFullMap = parts.get(grpId);

        for (GridDhtPartitionMap map : emptyFullMap.values())
            assertEquals(0, map.map().size());

        GridDhtPartitionFullMap fullMap = parts.get(dupGrpId);

        for (GridDhtPartitionMap map : fullMap.values())
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
        if (!F.isEmpty(msg.cacheGroupsAffinityRequest())) {
            for (GridDhtPartitionMap map : msg.partitions().values())
                assertTrue(F.isEmpty(map.map()));

            return;
        }

        int cache1Grp = groupIdForCache(ignite(0), cache1);
        int cache2Grp = groupIdForCache(ignite(0), cache2);

        Integer grpId;
        Integer dupGrpId;

        if (dupPartsData.containsKey(cache1Grp)) {
            grpId = cache1Grp;
            dupGrpId = cache2Grp;
        }
        else {
            grpId = cache2Grp;
            dupGrpId = cache1Grp;
        }

        assertTrue(dupPartsData.containsKey(grpId));
        assertEquals(dupGrpId, dupPartsData.get(grpId));
        assertFalse(dupPartsData.containsKey(dupGrpId));

        Map<Integer, GridDhtPartitionMap> parts = msg.partitions();

        GridDhtPartitionMap emptyMap = parts.get(grpId);

        assertEquals(0, emptyMap.map().size());

        GridDhtPartitionMap map = parts.get(dupGrpId);

        assertFalse(map.map().isEmpty());
    }

    /**
     *
     */
    private static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            // Do not start cache on coordinator.
            return !((String)node.attribute("name")).endsWith("0");
        }
    }
}
