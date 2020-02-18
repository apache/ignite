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
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFilterRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateInvokeRequest;
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
 * Tests single / transform messages being sent between nodes in ATOMIC mode.
 */
public class CacheAtomicSingleMessageCountSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        CacheConfiguration cCfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cCfg.setCacheMode(PARTITIONED);
        cCfg.setBackups(1);
        cCfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheConfiguration(cCfg);
        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleMessage() throws Exception {
        startClientGrid(0);
        startGrid(1);

        awaitPartitionMapExchange();

        TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(0).configuration().getCommunicationSpi();
        commSpi.resetCount();

        commSpi.registerMessage(GridNearAtomicFullUpdateRequest.class);
        commSpi.registerMessage(GridNearAtomicSingleUpdateRequest.class);
        commSpi.registerMessage(GridNearAtomicSingleUpdateInvokeRequest.class);
        commSpi.registerMessage(GridNearAtomicSingleUpdateFilterRequest.class);

        int putCnt = 15;

        for (int i = 0; i < putCnt; i++)
            jcache(0).put(i, i);

        assertEquals(0, commSpi.messageCount(GridNearAtomicFullUpdateRequest.class));
        assertEquals(putCnt, commSpi.messageCount(GridNearAtomicSingleUpdateRequest.class));
        assertEquals(0, commSpi.messageCount(GridNearAtomicSingleUpdateInvokeRequest.class));
        assertEquals(0, commSpi.messageCount(GridNearAtomicSingleUpdateFilterRequest.class));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleTransformMessage() throws Exception {
        startClientGrid(0);
        startGrid(1);

        int cacheId = ((IgniteKernal)grid(0)).internalCache(DEFAULT_CACHE_NAME).context().cacheId();

        awaitPartitionMapExchange();

        TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(0).configuration().getCommunicationSpi();

        commSpi.resetCount();
        commSpi.filterCacheId(cacheId);

        commSpi.registerMessage(GridNearAtomicFullUpdateRequest.class);
        commSpi.registerMessage(GridNearAtomicSingleUpdateRequest.class);
        commSpi.registerMessage(GridNearAtomicSingleUpdateInvokeRequest.class);
        commSpi.registerMessage(GridNearAtomicSingleUpdateFilterRequest.class);

        int putCnt = 15;

        for (int i = 0; i < putCnt; i++) {
            jcache(0).invoke(i, new CacheEntryProcessor<Object, Object, Object>() {
                @Override public Object process(MutableEntry<Object, Object> entry,
                    Object... objects) throws EntryProcessorException {
                    return 2;
                }
            });
        }

        assertEquals(0, commSpi.messageCount(GridNearAtomicFullUpdateRequest.class));
        assertEquals(0, commSpi.messageCount(GridNearAtomicSingleUpdateRequest.class));
        assertEquals(putCnt, commSpi.messageCount(GridNearAtomicSingleUpdateInvokeRequest.class));
        assertEquals(0, commSpi.messageCount(GridNearAtomicSingleUpdateFilterRequest.class));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleFilterMessage() throws Exception {
        startClientGrid(0);
        startGrid(1);

        awaitPartitionMapExchange();

        TestCommunicationSpi commSpi = (TestCommunicationSpi)grid(0).configuration().getCommunicationSpi();

        commSpi.resetCount();

        commSpi.registerMessage(GridNearAtomicFullUpdateRequest.class);
        commSpi.registerMessage(GridNearAtomicSingleUpdateRequest.class);
        commSpi.registerMessage(GridNearAtomicSingleUpdateInvokeRequest.class);
        commSpi.registerMessage(GridNearAtomicSingleUpdateFilterRequest.class);

        int putCnt = 15;

        for (int i = 0; i < putCnt; i++)
            jcache(0).putIfAbsent(i, i);

        assertEquals(0, commSpi.messageCount(GridNearAtomicFullUpdateRequest.class));
        assertEquals(0, commSpi.messageCount(GridNearAtomicSingleUpdateRequest.class));
        assertEquals(0, commSpi.messageCount(GridNearAtomicSingleUpdateInvokeRequest.class));
        assertEquals(putCnt, commSpi.messageCount(GridNearAtomicSingleUpdateFilterRequest.class));
    }

    /**
     * Test communication SPI.
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Counters map. */
        private Map<Class<?>, AtomicInteger> cntMap = new HashMap<>();

        /** Cache id to filter */
        private volatile Integer filterCacheId;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {

            if (((GridIoMessage)msg).message() instanceof GridCacheIdMessage) {
                int msgCacheId = ((GridCacheIdMessage)((GridIoMessage)msg).message()).cacheId();

                if (filterCacheId == null || filterCacheId == msgCacheId) {
                    AtomicInteger cntr = cntMap.get(((GridIoMessage)msg).message().getClass());

                    if (cntr != null)
                        cntr.incrementAndGet();
                }
            }

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
        void resetCount() {
            cntMap.clear();
            filterCacheId = null;
        }

        /**
         * @param cacheId Cache ID.
         */
        void filterCacheId(int cacheId) {
            filterCacheId = cacheId;
        }
    }
}
