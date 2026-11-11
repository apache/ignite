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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * The rebalance keys estimate comes from the supplier with the first supply message of a demand, the exchange full
 * message carries no partition sizes.
 */
public class RebalanceKeysEstimateTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS = 1000;

    /** */
    private boolean statsEnabled = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.REPLICATED)
                .setStatisticsEnabled(statsEnabled)
                .setRebalanceBatchSize(4 * 1024));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testEstimateComesWithFirstSupplyMessage() throws Exception {
        IgniteEx supplier = startGrid(0);

        for (int i = 0; i < KEYS; i++)
            supplier.cache(DEFAULT_CACHE_NAME).put(i, i);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(supplier);

        int grpId = CU.cacheId(DEFAULT_CACHE_NAME);

        spi.record(GridDhtPartitionsFullMessage.class, GridDhtPartitionSupplyMessage.class);
        spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage
            && ((GridDhtPartitionSupplyMessage)msg).groupId() == grpId);

        IgniteEx demander = startGrid(1);

        spi.waitForBlocked();

        // Rebalance has started, the estimate is not known until the supplier answers.
        assertEquals(0, demander.cache(DEFAULT_CACHE_NAME).localMetrics().getEstimatedRebalancingKeys());

        spi.stopBlock();

        awaitPartitionMapExchange();

        assertEquals(KEYS, demander.cache(DEFAULT_CACHE_NAME).localMetrics().getEstimatedRebalancingKeys());

        List<Object> recorded = spi.recordedMessages(true);

        for (GridDhtPartitionsFullMessage msg : filter(recorded, GridDhtPartitionsFullMessage.class))
            assertNull(msg.partitionSizes());

        List<GridDhtPartitionSupplyMessage> supplies = filter(recorded, GridDhtPartitionSupplyMessage.class).stream()
            .filter(msg -> msg.groupId() == grpId)
            .collect(Collectors.toList());

        assertTrue(supplies.size() > 1);

        assertEquals(KEYS, supplies.get(0).partSizes().values().stream().mapToLong(Long::longValue).sum());

        for (GridDhtPartitionSupplyMessage msg : supplies.subList(1, supplies.size()))
            assertNull(msg.partSizes());
    }

    /** */
    @Test
    public void testNoSizesWithoutStatistics() throws Exception {
        statsEnabled = false;

        IgniteEx supplier = startGrid(0);

        for (int i = 0; i < KEYS; i++)
            supplier.cache(DEFAULT_CACHE_NAME).put(i, i);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(supplier);

        spi.record(GridDhtPartitionSupplyMessage.class);

        startGrid(1);

        awaitPartitionMapExchange();

        List<GridDhtPartitionSupplyMessage> supplies = filter(spi.recordedMessages(true), GridDhtPartitionSupplyMessage.class);

        assertFalse(supplies.isEmpty());

        for (GridDhtPartitionSupplyMessage msg : supplies)
            assertNull(msg.partSizes());
    }

    /** */
    private static <T> List<T> filter(List<Object> msgs, Class<T> cls) {
        return msgs.stream().filter(cls::isInstance).map(cls::cast).collect(Collectors.toList());
    }
}
