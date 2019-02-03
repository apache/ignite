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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests specific scenario when binary metadata should be updated from a system thread
 * and topology has been already changed since the original transaction start.
 */
public class IgniteBinaryMetadataUpdateChangingTopologySelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(null);

        CacheConfiguration ccfg = new CacheConfiguration("cache");

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(4);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoDeadlockOptimistic() throws Exception {
        int key1 = primaryKey(ignite(1).cache("cache"));
        int key2 = primaryKey(ignite(2).cache("cache"));

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite(1).configuration().getCommunicationSpi();

        spi.blockMessages(GridNearTxPrepareResponse.class, ignite(0).cluster().localNode().id());

        IgniteCache<Object, Object> cache = ignite(0).cache("cache");

        IgniteFuture futPutAll = cache.putAllAsync(F.asMap(key1, "val1", key2, new TestValue1()));

        try {
            Thread.sleep(500);

            IgniteInternalFuture<Void> fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(4);

                    return null;
                }
            });

            Thread.sleep(1000);

            spi.stopBlock();

            futPutAll.get();

            fut.get();
        }
        finally {
            stopGrid(4);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoDeadlockInvoke() throws Exception {
        int key1 = primaryKey(ignite(1).cache("cache"));
        int key2 = primaryKey(ignite(2).cache("cache"));

        TestCommunicationSpi spi = (TestCommunicationSpi)ignite(1).configuration().getCommunicationSpi();

        spi.blockMessages(GridNearTxPrepareResponse.class, ignite(0).cluster().localNode().id());

        IgniteCache<Object, Object> cache = ignite(0).cache("cache");

        IgniteFuture futInvokeAll = cache.invokeAllAsync(F.asSet(key1, key2), new TestEntryProcessor());

        try {
            Thread.sleep(500);

            IgniteInternalFuture<Void> fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    startGrid(4);

                    return null;
                }
            });

            Thread.sleep(1000);

            spi.stopBlock();

            futInvokeAll.get();

            fut.get();
        }
        finally {
            stopGrid(4);
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private List<T2<ClusterNode, GridIoMessage>> blockedMsgs = new ArrayList<>();

        /** */
        private Map<Class<?>, Set<UUID>> blockCls = new HashMap<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                synchronized (this) {
                    Set<UUID> blockNodes = blockCls.get(msg0.getClass());

                    if (F.contains(blockNodes, node.id())) {
                        log.info("Block message [node=" +
                            node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME) + ", msg=" + msg0 + ']');

                        blockedMsgs.add(new T2<>(node, (GridIoMessage)msg));

                        return;
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }

        /**
         * @param cls Message class.
         * @param nodeId Node ID.
         */
        void blockMessages(Class<?> cls, UUID nodeId) {
            synchronized (this) {
                Set<UUID> set = blockCls.get(cls);

                if (set == null) {
                    set = new HashSet<>();

                    blockCls.put(cls, set);
                }

                set.add(nodeId);
            }
        }

        /**
         *
         */
        void stopBlock() {
            synchronized (this) {
                blockCls.clear();

                for (T2<ClusterNode, GridIoMessage> msg : blockedMsgs) {
                    ClusterNode node = msg.get1();

                    log.info("Send blocked message: [node=" +
                        node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME) +
                        ", msg=" + msg.get2().message() + ']');

                    sendMessage(msg.get1(), msg.get2());
                }

                blockedMsgs.clear();
            }
        }
    }

    /**
     *
     */
    static class TestEntryProcessor implements CacheEntryProcessor<Object, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> e, Object... arguments) {
            e.setValue(new TestValue2());

            return null;
        }
    }

    /**
     *
     */
    private static class TestValue1 {
        /** Field1. */
        private String field1;
    }

    /**
     *
     */
    private static class TestValue2 {
        /** Field1. */
        private String field1;
    }
}
