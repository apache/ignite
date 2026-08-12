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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;
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

    /** Direct type of the process request, past the core range. */
    private static final short TYPE = (short)(CoreMessagesProvider.MAX_MESSAGE_ID + 1);

    /** Direct type of the process result. */
    private static final short PAYLOAD_TYPE = (short)(CoreMessagesProvider.MAX_MESSAGE_ID + 2);

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

                    if (singleMsg.response() instanceof PayloadMessage)
                        PAYLOAD_BYTES.compareAndSet(null, ((PayloadMessage)singleMsg.response()).valBytes);
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "distributed-process-result-marshalling";
            }

            @Override public void initExtensions(PluginContext pluginCtx, ExtensionRegistry registry) {
                registry.registerExtension(MessageFactoryProvider.class, factory -> {
                    factory.register(TYPE, loadSerializer(TestIntegerMessage.class));
                    factory.register(PAYLOAD_TYPE, new PayloadSerializer(), new PayloadMarshaller());
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

        DistributedProcess<TestIntegerMessage, PayloadMessage> initiator = null;

        for (Ignite grid : G.allGrids()) {
            // An already finished future makes DistributedProcess send the result inline, on the discovery thread.
            DistributedProcess<TestIntegerMessage, PayloadMessage> p = new DistributedProcess<>(
                ((IgniteEx)grid).context(),
                TEST_PROCESS,
                (uuid, req) -> new GridFinishedFuture<>(new PayloadMessage(UUID.randomUUID())),
                (uuid, res, err) -> finishLatch.countDown()
            );

            if (grid == grid(0))
                initiator = p;
        }

        initiator.start(UUID.randomUUID(), new TestIntegerMessage(1));

        assertTrue("The process has not finished", finishLatch.await(TIMEOUT, MILLISECONDS));

        byte[] bytes = PAYLOAD_BYTES.get();

        assertNotNull("The payload was not marshalled, the test proves nothing", bytes);

        assertTrue("The result of a distributed process must be marshalled with the JDK marshaller on every transport, "
            + "so that the discovery leg reads the wire form the communication leg has cached", jdkStream(bytes));
    }

    /** @return {@code True} if the bytes are a JDK serialization stream. */
    private static boolean jdkStream(byte[] bytes) {
        return bytes.length > 1
            && bytes[0] == (byte)(ObjectStreamConstants.STREAM_MAGIC >> 8)
            && bytes[1] == (byte)ObjectStreamConstants.STREAM_MAGIC;
    }

    /**
     * Result of the process. It carries an object field, so that the payload really goes through a marshaller, and it
     * carries no {@code JdkMarshalled} of its own: the test checks that pinning {@link SingleNodeMessage} covers every
     * result, including the ones written later.
     */
    public static class PayloadMessage implements Message {
        /** Payload. */
        private Object val;

        /** Wire form of {@link #val}. */
        private byte[] valBytes;

        /** Default constructor for {@link MessageFactory}. */
        public PayloadMessage() {
            // No-op.
        }

        /** @param val Payload. */
        PayloadMessage(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            return PAYLOAD_TYPE;
        }
    }

    /** */
    private static class PayloadSerializer implements MessageSerializer<PayloadMessage> {
        /** {@inheritDoc} */
        @Override public boolean writeTo(PayloadMessage msg, MessageWriter writer) {
            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(msg.directType()))
                    return false;

                writer.onHeaderWritten();
            }

            return writer.writeByteArray(msg.valBytes);
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(PayloadMessage msg, MessageReader reader) {
            msg.valBytes = reader.readByteArray();

            return reader.isLastRead();
        }

        /** {@inheritDoc} */
        @Override public PayloadMessage createMessage() {
            return new PayloadMessage();
        }
    }

    /** Marshals the payload with whatever marshaller the caller hands over - the test asserts which one that is. */
    private static class PayloadMarshaller implements MessageMarshaller<PayloadMessage> {
        /** {@inheritDoc} */
        @Override public void marshal(PayloadMessage msg, Marshaller marsh, GridKernalContext kctx,
            CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
            if (msg.val != null && msg.valBytes == null)
                msg.valBytes = U.marshal(marsh, msg.val);
        }

        /** {@inheritDoc} */
        @Override public void unmarshal(PayloadMessage msg, Marshaller marsh, GridKernalContext kctx,
            CacheObjectContext cacheObjCtx, ClassLoader clsLdr) throws IgniteCheckedException {
            if (msg.valBytes != null) {
                msg.val = U.unmarshal(marsh, msg.valBytes, clsLdr);

                msg.valBytes = null;
            }
        }
    }
}
