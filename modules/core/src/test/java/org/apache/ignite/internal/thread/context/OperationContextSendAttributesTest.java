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

package org.apache.ignite.internal.thread.context;

import java.net.InetAddress;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.communication.IgniteIoTestMessage;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.discovery.tcp.messages.InetSocketAddressMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.lang.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class OperationContextSendAttributesTest extends GridCommonAbstractTest {
    /** */
    private PluginProvider pluginProvider;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        assert pluginProvider != null;

        cfg.setPluginProviders(pluginProvider);

        return cfg;
    }

    /** */
    @Test
    public void testSendAttributesByDiscovery() throws Exception {
        doTestOperationContextAttributesPropagation(true);
    }

    /** */
    @Test
    public void testSendAttributesByCommunication() throws Exception {
        doTestOperationContextAttributesPropagation(false);
    }

    /** */
    protected void doTestOperationContextAttributesPropagation(boolean discovery) throws Exception {
        OperationContextAttribute<InetSocketAddressMessage> dAttr1 =
            OperationContextAttribute.newInstance(new InetSocketAddressMessage(InetAddress.getLoopbackAddress(), 80));

        OperationContextAttribute<GridIntList> dAttr2 = OperationContextAttribute.newInstance(new GridIntList(1));

        OperationContextAttribute<GridByteArrayList> otherTestAttr = OperationContextAttribute.newInstance(new GridByteArrayList());

        pluginProvider = new AbstractTestPluginProvider() {
            @Override public String name() {
                return "TestDistributedOperationContextAttributesRegistrator";
            }

            @Override public void start(PluginContext ctx) {
                GridKernalContext kctx = ((IgniteEx)ctx.grid()).context();

                int dAttr1Id = OperationContextDispatcher.MAX_ATTRS_CNT - 2;
                int dAttr2Id = OperationContextDispatcher.MAX_ATTRS_CNT - 1;

                kctx.operationContextDispatcher().registerDistributedAttribute(dAttr1Id, dAttr1);
                kctx.operationContextDispatcher().registerDistributedAttribute(dAttr2Id, dAttr2);

                assertThrowsAnyCause(
                    log,
                    () -> {
                        kctx.operationContextDispatcher().registerDistributedAttribute(dAttr2Id, otherTestAttr);
                        return null;

                    }, IgniteException.class,
                    "Duplicated distributed attribute id"
                );
            }
        };

        // Local attribute 1.
        OperationContextAttribute.newInstance(1000);

        startGrids(2);
        startClientGrid(2);

        assertThrows(
            null,
            () -> grid(0).context().operationContextDispatcher().registerDistributedAttribute(1, null),
            IgniteException.class,
            "Initialization of distributed operation context attributes has already finished"
        );

        // Local attribute 2.
        OperationContextAttribute.newInstance("locaAttr2");

        InetSocketAddressMessage valToSend1 = new InetSocketAddressMessage(dAttr1.initialValue().address(), 443);
        GridIntList valToSend2 = new GridIntList(2);

        if (discovery)
            doTestOperationContextAttributesPropagationThroughDiscovery(dAttr1, valToSend1, dAttr2, valToSend2);
        else
            doTestOperationContextAttributesPropagationThroughCommunication(dAttr1, valToSend1, dAttr2, valToSend2);
    }

    /** */
    private void doTestOperationContextAttributesPropagationThroughDiscovery(
        OperationContextAttribute<InetSocketAddressMessage> dAttr1,
        InetSocketAddressMessage valToSend1,
        OperationContextAttribute<GridIntList> dAttr2,
        GridIntList valToSend2
    ) throws Exception {
        Set<Integer> checkedNodes = ConcurrentHashMap.newKeySet();

        for (int i = 0; i < G.allGrids().size(); ++i) {
            int i0 = i;

            grid(i).context().discovery().setCustomEventListener(
                DynamicCacheChangeBatch.class, new CustomEventListener<>() {
                    @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
                        DynamicCacheChangeBatch msg) {

                        InetSocketAddressMessage receivedVal1 = OperationContext.get(dAttr1);
                        GridIntList receivedVal2 = OperationContext.get(dAttr2);

                        assertTrue(receivedVal1 != null && valToSend1.port() == receivedVal1.port());
                        assertTrue(receivedVal1 != null && valToSend1.address().equals(receivedVal1.address()));

                        assertEquals(valToSend2, receivedVal2);

                        checkedNodes.add(i0);
                    }
                });
        }

        // Send from the coordinator.
        try (Scope ignored = OperationContext.set(dAttr1, valToSend1, dAttr2, valToSend2)) {
            grid(0).createCache(defaultCacheConfiguration());
        }

        assertTrue(waitForCondition(() -> checkedNodes.size() == 3, getTestTimeout(), 50));
        checkedNodes.clear();

        // Send from a server.
        try (Scope ignored = OperationContext.set(dAttr1, valToSend1, dAttr2, valToSend2)) {
            grid(1).destroyCache(DEFAULT_CACHE_NAME);
        }

        assertTrue(waitForCondition(() -> checkedNodes.size() == 3, getTestTimeout(), 50));
        checkedNodes.clear();

        // Send from a client.
        try (Scope ignored = OperationContext.set(dAttr1, valToSend1, dAttr2, valToSend2)) {
            grid(2).createCache(defaultCacheConfiguration());
        }

        assertTrue(waitForCondition(() -> checkedNodes.size() == 3, getTestTimeout(), 50));
        checkedNodes.clear();
    }

    /** */
    private void doTestOperationContextAttributesPropagationThroughCommunication(
        OperationContextAttribute<InetSocketAddressMessage> dAttr1,
        InetSocketAddressMessage valToSend1,
        OperationContextAttribute<GridIntList> dAttr2,
        GridIntList valToSend2
    ) throws Exception {
        // Coordinator -> Server, Coordinator -> Client, Server -> Client, Client -> Server, etc.
        for (int fromIdx = 0; fromIdx < 3; ++fromIdx) {
            for (int toIdx = 0; toIdx < 3; ++toIdx) {
                if (fromIdx == toIdx)
                    continue;

                // One value.
                try (Scope ignored = OperationContext.set(dAttr1, valToSend1)) {
                    checkOperationContextCommunicationTransmission(fromIdx, toIdx, dAttr1, null);
                }

                // A couple of values.
                try (Scope ignored = OperationContext.set(dAttr1, valToSend1, dAttr2, valToSend2)) {
                    checkOperationContextCommunicationTransmission(fromIdx, toIdx, dAttr1, dAttr2);
                }
            }
        }
    }

    /** */
    private void checkOperationContextCommunicationTransmission(
        int gridFromIdx,
        int gridToIdx,
        OperationContextAttribute<InetSocketAddressMessage> attr1,
        @Nullable OperationContextAttribute<GridIntList> attr2
    ) throws Exception {
        IgniteEx from = grid(gridFromIdx);
        IgniteEx to = grid(gridToIdx);

        CountDownLatch rcvLatch = new CountDownLatch(2);

        InetSocketAddressMessage expVal1 = OperationContext.get(attr1);
        GridIntList expVal2 = attr2 == null ? null : OperationContext.get(attr2);

        GridMessageListener lsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof IgniteIoTestMessage && ((IgniteIoTestMessage)msg).request()) {
                    InetSocketAddressMessage receivedVal1 = OperationContext.get(attr1);
                    GridIntList receivedVal2 = attr2 == null ? null : OperationContext.get(attr2);

                    assertTrue(receivedVal1 != null && expVal1.port() == receivedVal1.port());
                    assertTrue(receivedVal1 != null && expVal1.address().equals(receivedVal1.address()));

                    if (attr2 != null)
                        assertEquals(expVal2, receivedVal2);

                    rcvLatch.countDown();
                }
            }
        };

        to.context().io().addMessageListener(GridTopic.TOPIC_IO_TEST, lsnr);

        try {
            from.context().io().sendIoTest(node(from, to), null, false);
            from.context().io().sendIoTest(node(from, to), null, true);

            assertTrue(rcvLatch.await(getTestTimeout(), MILLISECONDS));
        }
        finally {
            assertTrue(to.context().io().removeMessageListener(GridTopic.TOPIC_IO_TEST, lsnr));
        }
    }

    /** Prevents {@link ClusterNode#isLocal()} to be negative. */
    private ClusterNode node(Ignite from, Ignite to) {
        return from.cluster().node(((IgniteEx)to).localNode().id());
    }
}
