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

package org.apache.ignite.internal.util.distributed;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Check {@link SingleNodeMessage} serialization. */
public class SingleNodeMessageSerializationTest extends GridCommonAbstractTest {
    /** Nodes count. */
    public static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setDiscoverySpi(new TestDiscoverySpi()
            .setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder()));

        return cfg;
    }

    /** Test check that serialization raised only once. */
    @Test
    public void testSingleSerializedOnce() throws Exception {
        startGridsMultiThreaded(NODES_CNT);
        startClientGrid(NODES_CNT);

        TestRecordingCommunicationSpi clnCommSpi = TestRecordingCommunicationSpi.spi(grid(NODES_CNT));

        assertTrue(grid(NODES_CNT).configuration().isClientMode());

        clnCommSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        TestDiscoverySpi discoSpi = (TestDiscoverySpi)grid(NODES_CNT).context().discovery().getInjectedDiscoverySpi();

        CountDownLatch latch = new CountDownLatch(1);

        discoSpi.messageLatch(latch);

        Set<UUID> nodeIdsRes = new HashSet<>();

        List<DistributedProcess<byte[], byte[]>> processes = new ArrayList<>(NODES_CNT + 1);

        for (int i = 0; i < NODES_CNT; i++)
            nodeIdsRes.add(grid(i).localNode().id());

        for (int n = 0; n < NODES_CNT + 1; n++) {
            DistributedProcess<byte[], byte[]> dp = new TestDistributedProcess(
                grid(n).context(), (id, req) -> new InitMessage<>(id, TEST_PROCESS, req, true));

            processes.add(dp);
        }

        int sendBuffSize = clnCommSpi.getSocketSendBuffer();

        // it will be enough for buffer overflow cause some serialization overhead is present
        byte[] arr = new byte[sendBuffSize];

        byte[] serialized = U.toBytes(arr);

        assertTrue(serialized.length > sendBuffSize);

        processes.get(0).start(UUID.randomUUID(), arr);

        clnCommSpi.waitForBlocked();

        assertEquals(1, clnCommSpi.blockedMessages().size());

        TestRecordingCommunicationSpi.BlockedMessageDescriptor blocked = clnCommSpi.blockedMessages().get(0);

        SingleNodeMessage msgSpied = (SingleNodeMessage)spy(blocked.ioMessage().message());

        setFieldValue(blocked.ioMessage(), "msg", msgSpied);

        clnCommSpi.stopBlock();

        latch.await(10, TimeUnit.SECONDS);

        // Serialized only once.
        verify(msgSpied, times(1)).toBytes(any());

        // Write to buffer - several times cause buffer size is less than serialization representation.
        verify(msgSpied, times(2)).writeTo(any(), any());
    }

    /** */
    private static class TestDistributedProcess extends DistributedProcess<byte[], byte[]> {
        /** */
        public TestDistributedProcess(
            GridKernalContext ctx,
            BiFunction<UUID, byte[], ? extends InitMessage<byte[]>> initMsgFactory
        ) {
            super(
                ctx,
                TEST_PROCESS,
                (req) -> new GridFinishedFuture<>(req),
                (uuid, res, err) -> {},
                initMsgFactory);
        }
    }

    /** */
    private static class TestDiscoverySpi extends TcpDiscoverySpi {
        /** */
        private CountDownLatch messageLatch;

        /** Message raized trigger. */
        void messageLatch(CountDownLatch messageLatch) {
            this.messageLatch = messageLatch;
        }

        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (messageLatch != null && msg instanceof TcpDiscoveryCustomEventMessage) {
                TcpDiscoveryCustomEventMessage discoMsg = (TcpDiscoveryCustomEventMessage)msg;

                try {
                    DiscoverySpiCustomMessage custMsg = discoMsg.message(marshaller(),
                        U.resolveClassLoader(ignite().configuration()));

                    if (custMsg instanceof CustomMessageWrapper) {
                        if (((CustomMessageWrapper)custMsg).delegate() instanceof FullMessage)
                            messageLatch.countDown();
                    }
                }
                catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }

            super.startMessageProcess(msg);
        }
    }
}
