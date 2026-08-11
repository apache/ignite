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

package org.apache.ignite.internal.managers.communication;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;

/**
 * Messages sent with {@link GridIoManager#sendOrderedMessage} are delivered strictly in the order of sending: the
 * receiver accumulates them per topic in a {@code GridCommunicationMessageSet} and drains it in one go
 * ({@code unwind}). "Ordered" here is about that delivery order and has nothing to do with the {@code @Order}
 * annotation of the direct protocol.
 * <p>
 * A message whose payload fails to unmarshal must be skipped with an error logged, not abandon the rest of the
 * accumulated set: the tail would otherwise stay unprocessed until the next message arrives on the topic.
 */
public class GridIoManagerOrderedUnmarshalFailureTest extends GridCommonAbstractTest {
    /** Direct type for the test message, past the core range. */
    private static final short TYPE = (short)(CoreMessagesProvider.MAX_MESSAGE_ID + 10);

    /** Ordered topic under test. */
    private static final String TOPIC = "unmarshal-failure-test-topic";

    /** Listener log of the receiver. */
    private ListeningTestLogger lsnrLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceIndex(igniteInstanceName) == 1)
            cfg.setGridLogger(lsnrLog);

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "ordered-unmarshal-failure-test";
            }

            @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
                registry.registerExtension(MessageFactoryProvider.class, factory ->
                    factory.register(TYPE, new Serializer(), new FailingMarshaller()));
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** A message failing to unmarshal between two valid ones must not prevent the delivery of the one behind it. */
    @Test
    public void testFailedMessageSkippedTailDelivered() throws Exception {
        lsnrLog = new ListeningTestLogger(log);

        LogListener skipLsnr = LogListener.matches("Failed to unmarshal ordered message (will skip)").build();

        lsnrLog.registerListener(skipLsnr);

        IgniteEx snd = startGrid(0);
        IgniteEx rcv = startGrid(1);

        Queue<Integer> delivered = new ConcurrentLinkedQueue<>();
        CountDownLatch firstReceived = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        CountDownLatch tailReceived = new CountDownLatch(1);

        rcv.context().io().addMessageListener(TOPIC, (nodeId, msg, plc) -> {
            FailingUnmarshalMessage m = (FailingUnmarshalMessage)msg;

            delivered.add(m.seq);

            if (m.seq == 1) {
                firstReceived.countDown();

                U.awaitQuiet(proceed);
            }
            else
                tailReceived.countDown();
        });

        GridIoManager sndIo = snd.context().io();

        // First message occupies the striped worker in the listener above.
        sndIo.sendOrderedMessage(rcv.localNode(), TOPIC, new FailingUnmarshalMessage(1, false), PUBLIC_POOL, 30_000, false);

        assertTrue("First message must be delivered", firstReceived.await(10, TimeUnit.SECONDS));

        // While the worker is blocked, the failing message and the one behind it accumulate in the same set.
        sndIo.sendOrderedMessage(rcv.localNode(), TOPIC, new FailingUnmarshalMessage(2, true), PUBLIC_POOL, 30_000, false);
        sndIo.sendOrderedMessage(rcv.localNode(), TOPIC, new FailingUnmarshalMessage(3, false), PUBLIC_POOL, 30_000, false);

        assertTrue("Both messages must reach the receiver's ordered set before the worker resumes",
            GridTestUtils.waitForCondition(() -> pendingOrderedMessages(rcv) == 2, 10_000));

        proceed.countDown();

        assertTrue("The message behind the failing one must still be delivered", tailReceived.await(10, TimeUnit.SECONDS));

        assertEquals("[1, 3]", delivered.toString());
        assertTrue("Skip must be logged", skipLsnr.check(10_000));
    }

    /** @return Number of messages pending in the receiver's ordered set of {@link #TOPIC}. */
    private static int pendingOrderedMessages(IgniteEx rcv) {
        Map<Object, Map<UUID, Object>> setMap = GridTestUtils.getFieldValue(rcv.context().io(), "msgSetMap");

        Map<UUID, Object> byNode = setMap.get(TOPIC);

        if (byNode == null || byNode.isEmpty())
            return 0;

        Queue<?> msgs = GridTestUtils.getFieldValue(byNode.values().iterator().next(), "msgs");

        return msgs.size();
    }

    /** Message with a sequence number and a flag making its payload unmarshal fail on the receiver. */
    private static class FailingUnmarshalMessage implements Message {
        /** */
        int seq;

        /** */
        boolean fail;

        /** */
        FailingUnmarshalMessage() {
            // No-op.
        }

        /** */
        FailingUnmarshalMessage(int seq, boolean fail) {
            this.seq = seq;
            this.fail = fail;
        }
    }

    /** Writes the two fields behind the header. */
    private static class Serializer implements MessageSerializer<FailingUnmarshalMessage> {
        /** {@inheritDoc} */
        @Override public boolean writeTo(FailingUnmarshalMessage msg, MessageWriter writer) {
            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(msg.directType()))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 0:
                    if (!writer.writeInt(msg.seq))
                        return false;

                    writer.incrementState();

                case 1:
                    if (!writer.writeBoolean(msg.fail))
                        return false;

                    writer.incrementState();
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(FailingUnmarshalMessage msg, MessageReader reader) {
            switch (reader.state()) {
                case 0:
                    msg.seq = reader.readInt();

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

                case 1:
                    msg.fail = reader.readBoolean();

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public FailingUnmarshalMessage createMessage() {
            return new FailingUnmarshalMessage();
        }
    }

    /** Fails the unmarshal of the flagged messages. */
    private static class FailingMarshaller implements MessageMarshaller<FailingUnmarshalMessage> {
        /** {@inheritDoc} */
        @Override public void marshal(FailingUnmarshalMessage msg, Marshaller marsh, GridKernalContext kctx,
            @Nullable CacheObjectContext cacheObjCtx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void unmarshal(FailingUnmarshalMessage msg, Marshaller marsh, GridKernalContext kctx,
            @Nullable CacheObjectContext cacheObjCtx, ClassLoader clsLdr) throws IgniteCheckedException {
            if (msg.fail)
                throw new IgniteCheckedException("Failed payload");
        }
    }
}
