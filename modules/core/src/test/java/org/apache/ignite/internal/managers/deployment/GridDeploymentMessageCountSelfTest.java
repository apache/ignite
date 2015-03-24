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

package org.apache.ignite.internal.managers.deployment;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jsr166.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Tests message count for different deployment scenarios.
 */
public class GridDeploymentMessageCountSelfTest extends GridCommonAbstractTest {
    /** VM ip finder for TCP discovery. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Test p2p task. */
    private static final String TEST_TASK = "org.apache.ignite.tests.p2p.SingleSplitTestTask";

    /** Test p2p value. */
    private static final String TEST_VALUE = "org.apache.ignite.tests.p2p.CacheDeploymentTestValue";

    /** SPIs. */
    private Map<String, MessageCountingCommunicationSpi> commSpis = new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setPeerClassLoadingEnabled(true);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cacheCfg);

        MessageCountingCommunicationSpi commSpi = new MessageCountingCommunicationSpi();

        commSpis.put(gridName, commSpi);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    public void testTaskDeployment() throws Exception {
        ClassLoader ldr = getExternalClassLoader();

        Class taskCls = ldr.loadClass(TEST_TASK);

        try {
            startGrids(2);

            ComputeTaskFuture<Object> taskFut = executeAsync(grid(0).compute(), taskCls, 2);

            Integer res = (Integer)taskFut.get();

            assertEquals(Integer.valueOf(2), res);

            for (MessageCountingCommunicationSpi spi : commSpis.values()) {
                assertTrue(spi.deploymentMessageCount() > 0);

                spi.resetCount();
            }

            for (int i = 0; i < 10; i++) {
                taskFut = executeAsync(grid(0).compute(), taskCls, 2);

                res = (Integer)taskFut.get();

                assertEquals(Integer.valueOf(2), res);
            }

            for (MessageCountingCommunicationSpi spi : commSpis.values())
                assertEquals(0, spi.deploymentMessageCount());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheValueDeploymentOnPut() throws Exception {
        ClassLoader ldr = getExternalClassLoader();

        Class valCls = ldr.loadClass(TEST_VALUE);

        try {
            startGrids(2);

            IgniteCache<Object, Object> cache = grid(0).cache(null);

            cache.put("key", valCls.newInstance());

            for (int i = 0; i < 2; i++)
                assertNotNull("For grid: " + i, grid(i).cache(null).localPeek("key", CachePeekMode.ONHEAP));

            for (MessageCountingCommunicationSpi spi : commSpis.values()) {
                assertTrue(spi.deploymentMessageCount() > 0);

                spi.resetCount();
            }

            for (int i = 0; i < 10; i++) {
                String key = "key" + i;

                cache.put(key, valCls.newInstance());

                for (int k = 0; k < 2; k++)
                    assertNotNull(grid(k).cache(null).localPeek(key, CachePeekMode.ONHEAP));
            }

            for (MessageCountingCommunicationSpi spi : commSpis.values())
                assertEquals(0, spi.deploymentMessageCount());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private class MessageCountingCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private AtomicInteger msgCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg)
            throws IgniteSpiException {
            if (isDeploymentMessage((GridIoMessage)msg))
                msgCnt.incrementAndGet();

            super.sendMessage(node, msg);
        }

        /**
         * @return Number of deployment messages.
         */
        public int deploymentMessageCount() {
            return msgCnt.get();
        }

        /**
         * Resets counter to zero.
         */
        public void resetCount() {
            msgCnt.set(0);
        }

        /**
         * Checks if it is a p2p deployment message.
         *
         * @param msg Message to check.
         * @return {@code True} if this is a p2p message.
         */
        private boolean isDeploymentMessage(GridIoMessage msg) {
            Object origMsg = msg.message();

            boolean dep = (origMsg instanceof GridDeploymentRequest) || (origMsg instanceof GridDeploymentResponse);

            if (dep)
                info(">>> Got deployment message: " + origMsg);

            return dep;
        }
    }
}
