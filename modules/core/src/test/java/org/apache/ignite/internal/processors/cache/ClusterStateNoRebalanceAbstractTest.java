/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static com.google.common.base.Functions.identity;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/**
 * Checks that join node to {@link ClusterState#INACTIVE} cluster doesn't trigger rebalance.
 */
public abstract class ClusterStateNoRebalanceAbstractTest extends GridCommonAbstractTest {
    /** Entry count. */
    protected static final int ENTRY_CNT = 5000;

    /** */
    protected static final Collection<Class> forbidden = new GridConcurrentHashSet<>();

    /** */
    private static AtomicReference<Exception> errEncountered = new AtomicReference<>();

    /** */
    private static final int GRID_CNT = 2;

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected abstract CacheConfiguration cacheConfiguration(String cacheName);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setActiveOnStart(false);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        cfg.setClientMode(gridName.startsWith("client"));

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        forbidden.clear();

        Exception err = errEncountered.getAndSet(null);

        if (err != null)
            throw err;

        super.afterTest();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNoRebalancing() throws Exception {
        forbidden.add(GridDhtPartitionSupplyMessage.class);
        forbidden.add(GridDhtPartitionDemandMessage.class);

        startGrids(GRID_CNT);

        checkInactive(GRID_CNT);

        forbidden.clear();

        grid(0).cluster().state(ACTIVE);

        awaitPartitionMapExchange();

        final IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.putAll(IntStream.range(0, ENTRY_CNT).boxed().collect(Collectors.toMap(identity(), identity())));

        for (int g = 0; g < GRID_CNT; g++) {
            // Tests that state changes are propagated to existing and new nodes.
            assertEquals(ACTIVE, grid(g).cluster().state());

            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals(k, grid(g).cache(DEFAULT_CACHE_NAME).get(k));
        }

        // Check that new node startup and shutdown works fine after activation.
        startGrid(GRID_CNT);
        startGrid(GRID_CNT + 1);

        for (int g = 0; g < GRID_CNT + 2; g++) {
            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals("Failed for [grid=" + g + ", key=" + k + ']', k, grid(g).cache(DEFAULT_CACHE_NAME).get(k));
        }

        stopGrid(GRID_CNT + 1);

        for (int g = 0; g < GRID_CNT + 1; g++)
            grid(g).cache(DEFAULT_CACHE_NAME).rebalance().get();

        stopGrid(GRID_CNT);

        for (int g = 0; g < GRID_CNT; g++) {
            IgniteCache<Object, Object> cache0 = grid(g).cache(DEFAULT_CACHE_NAME);

            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals(k, cache0.get(k));
        }

        grid(0).cluster().state(INACTIVE);

        checkInactive(GRID_CNT);

        forbidden.add(GridDhtPartitionSupplyMessage.class);
        forbidden.add(GridDhtPartitionDemandMessage.class);

        // Should stop without exchange.
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNoRebalancingWithClient() throws Exception {
        forbidden.add(GridDhtPartitionSupplyMessage.class);
        forbidden.add(GridDhtPartitionDemandMessage.class);

        startGrids(GRID_CNT);

        IgniteEx client = startGrid("client");

        assertTrue(client.configuration().isClientMode());

        checkInactive(GRID_CNT);

        assertEquals(INACTIVE, client.cluster().state());
    }

    /** */
    void checkInactive(int cnt) {
        for (int g = 0; g < cnt; g++)
            assertEquals(grid(g).name(), INACTIVE, grid(g).cluster().state());
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            checkForbidden((GridIoMessage)msg);

            super.sendMessage(node, msg, ackC);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            checkForbidden((GridIoMessage)msg);

            super.sendMessage(node, msg);
        }

        /**
         * @param msg Message to check.
         */
        private void checkForbidden(GridIoMessage msg) {
            if (forbidden.contains(msg.message().getClass())) {
                IgniteSpiException err = new IgniteSpiException("Message is forbidden for this test: " + msg.message());

                // Set error in case if this exception is not visible to the user code.
                errEncountered.compareAndSet(null, err);

                throw err;
            }
        }
    }
}
