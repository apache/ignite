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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.communication.IgniteIoTestMessage;
import org.apache.ignite.internal.processors.authentication.User;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.security.TestDiscoveryAcknowledgeMessage;
import org.apache.ignite.internal.processors.security.TestDiscoveryMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.MessagesPluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.GridTopic.TOPIC_IO_TEST;
import static org.apache.ignite.internal.thread.context.OperationContextAttribute.newInstance;
import static org.apache.ignite.internal.thread.context.OperationContextAttributePropagationTest.TestIgniteComponent.DFLT_PTR;
import static org.apache.ignite.internal.thread.context.OperationContextAttributePropagationTest.TestIgniteComponent.DFLT_USR;
import static org.apache.ignite.internal.thread.context.OperationContextAttributePropagationTest.TestIgniteComponent.PTR_ATTR;
import static org.apache.ignite.internal.thread.context.OperationContextAttributePropagationTest.TestIgniteComponent.USR_ATTR;
import static org.apache.ignite.internal.thread.context.OperationContextDispatcher.MAX_ATTRS_CNT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class OperationContextAttributePropagationTest extends GridCommonAbstractTest {
    /** */
    private volatile Consumer<Integer> discoMsgLsnr;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPluginProviders(
            new TestIgniteComponent(),
            new MessagesPluginProvider(TestDiscoveryMessage.class, TestDiscoveryAcknowledgeMessage.class));

        return cfg;
    }

    /** */
    @Test
    public void testSendAttributesByDiscovery() throws Exception {
        prepareCluster();

        doTestOperationContextAttributesPropagationThroughDiscovery(new WALPointer(1, 1, 1), User.create("1", "1"));
    }

    /** */
    @Test
    public void testSendAttributesByCommunication() throws Exception {
        prepareCluster();

        doTestOperationContextAttributesPropagationThroughCommunication(new WALPointer(1, 1, 1), User.create("1", "1"));
    }

    /** */
    private void prepareCluster() throws Exception {
        startGrids(2);
        startClientGrid(2);

        assertThrows(
            null,
            () -> grid(0).context().operationContextDispatcher().registerDistributedAttribute(1, null),
            IgniteException.class,
            "Initialization of distributed operation context attributes has already finished"
        );

        for (int nodeIdx = 0; nodeIdx < 3; nodeIdx++) {
            int finalNodeIdx = nodeIdx;

            grid(nodeIdx).context().discovery().setCustomEventListener(TestDiscoveryMessage.class, (topVer, snd, msg) -> {
                if (discoMsgLsnr != null)
                    discoMsgLsnr.accept(finalNodeIdx);
            });

            grid(nodeIdx).context().discovery().setCustomEventListener(TestDiscoveryAcknowledgeMessage.class, (topVer, snd, msg) -> {
                if (discoMsgLsnr != null)
                    discoMsgLsnr.accept(finalNodeIdx);
            });
        }
    }

    /** */
    private void doTestOperationContextAttributesPropagationThroughDiscovery(WALPointer ptrVal, User usrVal) throws Exception {
        for (int nodeIdx = 0; nodeIdx < G.allGrids().size(); ++nodeIdx) {
            try (Scope ignored = OperationContext.set(PTR_ATTR, ptrVal)) {
                checkOperationContextDiscoveryTransmission(nodeIdx, ptrVal, DFLT_USR);
            }

            try (Scope ignored = OperationContext.set(USR_ATTR, usrVal)) {
                checkOperationContextDiscoveryTransmission(nodeIdx, DFLT_PTR, usrVal);
            }

            try (Scope ignored = OperationContext.set(PTR_ATTR, ptrVal, USR_ATTR, usrVal)) {
                checkOperationContextDiscoveryTransmission(nodeIdx, ptrVal, usrVal);
            }

            checkOperationContextDiscoveryTransmission(nodeIdx, DFLT_PTR, DFLT_USR);
        }
    }

    /** */
    private void doTestOperationContextAttributesPropagationThroughCommunication(WALPointer ptrVal, User usrVal) throws Exception {
        for (int fromIdx = 0; fromIdx < 3; ++fromIdx) {
            for (int toIdx = 0; toIdx < 3; ++toIdx) {
                if (fromIdx == toIdx)
                    continue;

                try (Scope ignored = OperationContext.set(PTR_ATTR, ptrVal)) {
                    checkOperationContextCommunicationTransmission(fromIdx, toIdx, ptrVal, DFLT_USR);
                }

                try (Scope ignored = OperationContext.set(USR_ATTR, usrVal)) {
                    checkOperationContextCommunicationTransmission(fromIdx, toIdx, DFLT_PTR, usrVal);
                }

                try (Scope ignored = OperationContext.set(PTR_ATTR, ptrVal, USR_ATTR, usrVal)) {
                    checkOperationContextCommunicationTransmission(fromIdx, toIdx, ptrVal, usrVal);
                }

                checkOperationContextCommunicationTransmission(fromIdx, toIdx, DFLT_PTR, DFLT_USR);
            }
        }
    }

    /** */
    private void checkOperationContextDiscoveryTransmission(int sndIdx, WALPointer expPtrVal, User expUsrVal) throws Exception {
        Map<Integer, AtomicInteger> checkedNodes = new ConcurrentHashMap<>();

        discoMsgLsnr = nodeIdx -> {
            assertEquals(expUsrVal, OperationContext.get(USR_ATTR));
            assertEquals(expPtrVal, OperationContext.get(PTR_ATTR));

            checkedNodes.computeIfAbsent(nodeIdx, k -> new AtomicInteger()).incrementAndGet();
        };

        try {
            grid(sndIdx).context().discovery().sendCustomEvent(new TestDiscoveryMessage());

            assertTrue(waitForCondition(() ->
                checkedNodes.size() == 3 &&
                checkedNodes.values().stream().mapToInt(AtomicInteger::get).allMatch(v -> v == 2),
                getTestTimeout(),
                50));
        }
        finally {
            discoMsgLsnr = null;
        }
    }

    /** */
    private void checkOperationContextCommunicationTransmission(
        int fromIdx,
        int toIdx,
        WALPointer expPtrVal,
        User expUsrVal
    ) throws Exception {
        IgniteEx from = grid(fromIdx);
        IgniteEx to = grid(toIdx);

        CountDownLatch rcvLatch = new CountDownLatch(2);

        GridMessageListener lsnr = (nodeId, msg, plc) -> {
            if (msg instanceof IgniteIoTestMessage && ((IgniteIoTestMessage)msg).request()) {
                assertEquals(expUsrVal, OperationContext.get(USR_ATTR));
                assertEquals(expPtrVal, OperationContext.get(PTR_ATTR));

                rcvLatch.countDown();
            }
        };

        to.context().io().addMessageListener(TOPIC_IO_TEST, lsnr);

        try {
            from.context().io().ioTest().sendIoTest(node(from, to), null, false);
            from.context().io().ioTest().sendIoTest(node(from, to), null, true);

            assertTrue(rcvLatch.await(getTestTimeout(), MILLISECONDS));
        }
        finally {
            assertTrue(to.context().io().removeMessageListener(TOPIC_IO_TEST, lsnr));
        }
    }

    /** Prevents {@link ClusterNode#isLocal()} to be negative. */
    private ClusterNode node(Ignite from, Ignite to) {
        return from.cluster().node(((IgniteEx)to).localNode().id());
    }

    /** */
    static class TestIgniteComponent extends AbstractTestPluginProvider {
        /** */
        public static final WALPointer DFLT_PTR = new WALPointer(0, 0, 0);

        /** */
        public static final User DFLT_USR = User.create("0", "0");

        /** */
        public static final OperationContextAttribute<WALPointer> PTR_ATTR = newInstance(DFLT_PTR);

        /** */
        public static final OperationContextAttribute<User> USR_ATTR = newInstance(DFLT_USR);

        /** {@inheritDoc} */
        @Override public String name() {
            return "TestDistributedOperationContextAttributesRegistrator";
        }

        /** {@inheritDoc} */
        @Override public void start(PluginContext ctx) {
            GridKernalContext kctx = ((IgniteEx)ctx.grid()).context();

            kctx.operationContextDispatcher().registerDistributedAttribute(MAX_ATTRS_CNT - 1, USR_ATTR);
            kctx.operationContextDispatcher().registerDistributedAttribute(0, PTR_ATTR);

            assertThrowsAnyCause(
                log,
                () -> {
                    kctx.operationContextDispatcher().registerDistributedAttribute(MAX_ATTRS_CNT - 1, PTR_ATTR);
                    return null;

                }, IgniteException.class,
                "Duplicated distributed attribute id"
            );
        }
    }
}
