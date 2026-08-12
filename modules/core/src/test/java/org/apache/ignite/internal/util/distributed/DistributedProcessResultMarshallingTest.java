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

import java.io.ObjectStreamConstants;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;
import static org.apache.ignite.testframework.GridTestUtils.loadMarshaller;
import static org.apache.ignite.testframework.GridTestUtils.loadSerializer;

/**
 * Tests that the result of a {@link DistributedProcess} is marshalled with the JDK marshaller on both transports: it
 * goes to the coordinator by communication and comes back in the {@link FullMessage} by discovery, while a marshalled
 * field caches its wire form for the second leg.
 */
public class DistributedProcessResultMarshallingTest extends GridCommonAbstractTest {
    /** Timeout to wait latches. */
    private static final long TIMEOUT = 20_000L;

    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Wire form of the process result, captured on its way out. */
    private static final AtomicReference<byte[]> PAYLOAD_BYTES = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // A message is marshalled by the sending thread right before the SPI call, so the payload is ready by now.
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof SingleNodeMessage) {
                    SingleNodeMessage<?> singleMsg = (SingleNodeMessage<?>)((GridIoMessage)msg).message();

                    if (singleMsg.response() instanceof TestPayloadMessage)
                        PAYLOAD_BYTES.compareAndSet(null, ((TestPayloadMessage)singleMsg.response()).valueBytes());
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        // MessagesPluginProvider registers a serializer only, while the payload needs its marshaller companion too.
        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "distributed-process-marshalling-test";
            }

            @Override public void initExtensions(PluginContext pluginCtx, ExtensionRegistry registry) {
                registry.registerExtension(MessageFactoryProvider.class, factory -> {
                    factory.register((short)(CoreMessagesProvider.MAX_MESSAGE_ID + 1),
                        loadSerializer(TestIntegerMessage.class));

                    factory.register((short)(CoreMessagesProvider.MAX_MESSAGE_ID + 2),
                        loadSerializer(TestPayloadMessage.class), loadMarshaller(TestPayloadMessage.class));
                });
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        PAYLOAD_BYTES.set(null);

        super.afterTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testResultIsMarshalledWithJdkOnCommunication() throws Exception {
        startGrids(NODES_CNT);

        CountDownLatch finishLatch = new CountDownLatch(NODES_CNT);

        Map<String, DistributedProcess<TestIntegerMessage, TestPayloadMessage>> processes = new HashMap<>();

        for (Ignite grid : G.allGrids()) {
            DistributedProcess<TestIntegerMessage, TestPayloadMessage> p = new DistributedProcess<>(
                ((IgniteEx)grid).context(),
                TEST_PROCESS,
                // An already finished future makes DistributedProcess send the result inline, on the discovery thread.
                (uuid, req) -> new GridFinishedFuture<>(new TestPayloadMessage(UUID.randomUUID())),
                (uuid, res, err) -> finishLatch.countDown()
            );

            processes.put(grid.name(), p);
        }

        processes.get(grid(0).name()).start(UUID.randomUUID(), new TestIntegerMessage(1));

        assertTrue("The process has not finished", finishLatch.await(TIMEOUT, MILLISECONDS));

        byte[] bytes = PAYLOAD_BYTES.get();

        assertNotNull("The payload was not marshalled, the test proves nothing", bytes);

        assertTrue("The result of a distributed process must be marshalled with the JDK marshaller on every transport, "
                + "so that the discovery leg reads the wire form the communication leg has cached",
            bytes.length > 1
                && bytes[0] == (byte)(ObjectStreamConstants.STREAM_MAGIC >> 8)
                && bytes[1] == (byte)ObjectStreamConstants.STREAM_MAGIC);
    }
}
