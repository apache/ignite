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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PREFER_WAL_REBALANCE;

/** */
@WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
public class MultiDcRebalancingTest extends GridCommonAbstractTest {
    /** */
    private static final String DC1 = "DC1";

    /** */
    private static final String DC2 = "DC2";

    /** */
    private boolean pds;

    /** */
    private IgniteEx startGrid(int idx, String dcId) throws Exception {
        return startGrid(getConfiguration(getTestIgniteInstanceName(idx))
            .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(pds)))
            .setConsistentId(getTestIgniteInstanceName(idx))
            .setCommunicationSpi(new RebalanceAwareCommSPI())
            .setUserAttributes(F.asMap(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, dcId)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testFullRebalanceSameDC() throws Exception {
        checkFullRebalance(false);
    }

    /** */
    @Test
    public void testFullRebalanceCrossDC() throws Exception {
        checkFullRebalance(true);
    }

    /** */
    @Test
    public void testHistoricalRebalanceSameDC() throws Exception {
        checkHistoricalRebalance(false);
    }

    /** */
    @Test
    public void testHistoricalRebalanceCrossDC() throws Exception {
        checkHistoricalRebalance(true);
    }

    /** */
    private void checkFullRebalance(boolean crossDC) throws Exception {
        startGrid(0, DC1);
        startGrid(1, crossDC ? DC1 : DC2);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        IgniteCache<Object, Object> cache = grid(0).createCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setBackups(1));

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        IgniteEx ignite2 = startGrid(2, crossDC ? DC2 : DC1);
        IgniteEx ignite3 = startGrid(3, DC2);

        resetBaselineTopology();

        waitRebalanceFinished(ignite2, DEFAULT_CACHE_NAME);
        waitRebalanceFinished(ignite3, DEFAULT_CACHE_NAME);

        assertTrue(commSPI(ignite2).rebalanceMsgCnt > 0);
        assertTrue(commSPI(ignite3).rebalanceMsgCnt > 0);
        assertFalse(commSPI(ignite2).historical);
        assertFalse(commSPI(ignite3).historical);
        assertEquals(crossDC, commSPI(ignite2).crossDcRebalance);
        assertEquals(crossDC, commSPI(ignite3).crossDcRebalance);
    }

    /** */
    private void checkHistoricalRebalance(boolean crossDC) throws Exception {
        pds = true;

        startGrid(0, DC1);
        startGrid(1, DC2);
        startGrid(2, DC1);

        grid(0).cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = grid(0).createCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setBackups(crossDC ? 1 : 2));

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        forceCheckpoint();

        stopGrid(2);

        for (int i = 10_000; i < 20_000; i++)
            cache.put(i, i);

        IgniteEx ignite2 = startGrid(2, DC1);

        waitRebalanceFinished(ignite2, DEFAULT_CACHE_NAME);

        assertTrue(commSPI(ignite2).rebalanceMsgCnt > 0);
        assertTrue(commSPI(ignite2).historical);
        assertEquals(crossDC, commSPI(ignite2).crossDcRebalance);
    }

    /** */
    private RebalanceAwareCommSPI commSPI(Ignite ignite) {
        return (RebalanceAwareCommSPI)(ignite.configuration().getCommunicationSpi());
    }

    /** */
    private static class RebalanceAwareCommSPI extends TcpCommunicationSpi {
        /** */
        private int rebalanceMsgCnt;

        /** */
        private boolean historical;

        /** */
        private boolean crossDcRebalance;

        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackClosure
        ) throws IgniteSpiException {
            if (((GridIoMessage)msg).message() instanceof GridDhtPartitionDemandMessage) {
                rebalanceMsgCnt++;

                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)((GridIoMessage)msg).message();

                historical |= demandMsg.partitions().hasHistorical();

                if (!ignite.cluster().localNode().dataCenterId().equals(node.dataCenterId()))
                    crossDcRebalance = true;
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }
}
