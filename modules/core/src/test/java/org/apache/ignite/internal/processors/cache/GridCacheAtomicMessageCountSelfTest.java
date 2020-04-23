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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests messages being sent between nodes in ATOMIC mode.
 */
public class GridCacheAtomicMessageCountSelfTest extends GridCommonAbstractTest {
    /** Starting grid index. */
    private int idx;

    /** Client mode flag. */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        CacheConfiguration cCfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cCfg.setCacheMode(PARTITIONED);
        cCfg.setBackups(1);
        cCfg.setWriteSynchronizationMode(FULL_SYNC);

        if (idx == 0 && client)
            cfg.setClientMode(true);

        idx++;

        cfg.setCacheConfiguration(cCfg);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitioned() throws Exception {
        checkMessages(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClient() throws Exception {
        checkMessages(true);
    }

    /**
     * @param clientMode Client mode flag.
     * @throws Exception If failed.
     */
    protected void checkMessages(boolean clientMode) throws Exception {
        client = clientMode;

        startGrids(4);

        ignite(0).cache(DEFAULT_CACHE_NAME);

        try {
            awaitPartitionMapExchange();

            TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(0).configuration().getCommunicationSpi();

            commSpi.registerMessage(GridNearAtomicSingleUpdateRequest.class);
            commSpi.registerMessage(GridNearAtomicFullUpdateRequest.class);
            commSpi.registerMessage(GridDhtAtomicUpdateRequest.class);
            commSpi.registerMessage(GridDhtAtomicSingleUpdateRequest.class);

            int putCnt = 15;

            int expNearSingleCnt = 0;
            int expNearCnt = 0;
            int expDhtCnt = 0;

            for (int i = 0; i < putCnt; i++) {
                ClusterNode locNode = grid(0).localNode();

                Affinity<Object> affinity = ignite(0).affinity(DEFAULT_CACHE_NAME);

                if (affinity.isPrimary(locNode, i))
                    expDhtCnt++;
                else
                    expNearSingleCnt++;

                jcache(0).put(i, i);
            }

            assertEquals(expNearCnt, commSpi.messageCount(GridNearAtomicFullUpdateRequest.class));
            assertEquals(expNearSingleCnt, commSpi.messageCount(GridNearAtomicSingleUpdateRequest.class));
            assertEquals(expDhtCnt, commSpi.messageCount(GridDhtAtomicSingleUpdateRequest.class));

            for (int i = 1; i < 4; i++) {
                commSpi = (TestCommunicationSpi)grid(i).configuration().getCommunicationSpi();

                assertEquals(0, commSpi.messageCount(GridNearAtomicSingleUpdateRequest.class));
                assertEquals(0, commSpi.messageCount(GridNearAtomicFullUpdateRequest.class));
            }

        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test communication SPI.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Counters map. */
        private Map<Class<?>, AtomicInteger> cntMap = new HashMap<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            AtomicInteger cntr = cntMap.get(((GridIoMessage)msg).message().getClass());

            if (cntr != null)
                cntr.incrementAndGet();

            super.sendMessage(node, msg, ackC);
        }

        /**
         * Registers message for counting.
         *
         * @param cls Class to count.
         */
        void registerMessage(Class<?> cls) {
            AtomicInteger cntr = cntMap.get(cls);

            if (cntr == null)
                cntMap.put(cls, new AtomicInteger());
        }

        /**
         * @param cls Message type to get count.
         * @return Number of messages of given class.
         */
        int messageCount(Class<?> cls) {
            AtomicInteger cntr = cntMap.get(cls);

            return cntr == null ? 0 : cntr.get();
        }

        /**
         * Resets counter to zero.
         */
        public void resetCount() {
            cntMap.clear();
        }
    }
}
