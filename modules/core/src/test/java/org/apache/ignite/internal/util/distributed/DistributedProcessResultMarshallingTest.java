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
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.CommunicationMarshalling;
import org.apache.ignite.internal.managers.communication.MessageMarshalling;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.ErrorMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
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

    /** Marshallers the payload of the process result was handed, on every leg it travels. */
    private static final Collection<Marshaller> PAYLOAD_MARSH = new ConcurrentLinkedQueue<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

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

        PAYLOAD_MARSH.clear();

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

        // The result goes to the coordinator by communication and comes back in the FullMessage by discovery.
        assertTrue("The payload was marshalled on one leg only, the test proves nothing: " + PAYLOAD_MARSH,
            PAYLOAD_MARSH.size() > 1);

        for (Marshaller marsh : PAYLOAD_MARSH) {
            assertTrue("The result of a distributed process must be marshalled with the JDK marshaller on every "
                + "transport, so that the discovery leg reads the wire form the communication leg has cached, but got "
                + marsh, marsh instanceof JdkMarshaller);
        }
    }


    /**
     * A pin belongs to the message that carries it, so a pinned message nested into a message that is not pinned is
     * still marshalled with the JDK marshaller, whatever its parent got from the transport.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNestedPinnedMessageKeepsItsMarshaller() throws Exception {
        startGrids(NODES_CNT);

        PayloadMessage msg = new PayloadMessage(UUID.randomUUID());

        // Communication hands the schema-aware marshaller over, and the payload of the parent takes it.
        CommunicationMarshalling.marshal(msg, grid(0).context(), null);

        assertTrue("The parent was marshalled with " + PAYLOAD_MARSH, PAYLOAD_MARSH.stream().noneMatch(m -> m instanceof JdkMarshaller));

        assertNotNull("The nested message was not marshalled", msg.err.errBytes);

        assertTrue("A pinned message nested into a plain one must still go through the JDK marshaller",
            msg.err.errBytes.length > 1
                && msg.err.errBytes[0] == (byte)(ObjectStreamConstants.STREAM_MAGIC >> 8)
                && msg.err.errBytes[1] == (byte)ObjectStreamConstants.STREAM_MAGIC);
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

        /** A pinned message nested into a message that is not pinned itself. */
        private ErrorMessage err;

        /** Default constructor for {@link MessageFactory}. */
        public PayloadMessage() {
            // No-op.
        }

        /** @param val Payload. */
        PayloadMessage(Object val) {
            this.val = val;
            this.err = new ErrorMessage(new IllegalStateException("nested"));
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

            switch (writer.state()) {
                case 0:
                    if (!writer.writeByteArray(msg.valBytes))
                        return false;

                    writer.incrementState();

                case 1:
                    if (!writer.writeMessage(msg.err))
                        return false;

                    writer.incrementState();
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(PayloadMessage msg, MessageReader reader) {
            switch (reader.state()) {
                case 0:
                    msg.valBytes = reader.readByteArray();

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

                case 1:
                    msg.err = reader.readMessage();

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();
            }

            return true;
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
            PAYLOAD_MARSH.add(marsh);

            if (msg.val != null && msg.valBytes == null)
                msg.valBytes = U.marshal(marsh, msg.val);

            if (msg.err != null)
                MessageMarshalling.marshal(msg.err, marsh, kctx, cacheObjCtx);
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
