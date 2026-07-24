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

package org.apache.ignite.plugin.extensions.communication;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Verifies the marshal-once-before-fan-out contract of the collection {@code send}: a message broadcast to N remote
 * nodes is marshalled <b>exactly once</b> (prepared before the fan-out and reused for every destination), not once per
 * destination. A counting marshaller is registered for a test message; broadcasting it to {@link #RMT_CNT} remote nodes
 * must produce {@code RMT_CNT} sends but a single {@code marshal}. Counting the sends keeps the check honest (a
 * lone send would also marshal once). The unmarshal-once counterpart lives in {@link MessageUnmarshalOnceTest}.
 *
 * <p>The second scenario covers the send-retry path: a cache message whose first send attempt fails is retried,
 * and the retry must not re-marshal the payload (it is prepared once by {@code GridCacheIoManager} before the
 * retry loop, while {@code GridIoManager} marshals only the wrapper).
 */
public class MessageMarshalOnceTest extends GridCommonAbstractTest {
    /** Direct type for the test message, past the core range. */
    private static final short TYPE = (short)(CoreMessagesProvider.MAX_MESSAGE_ID + 1);

    /** Direct type for the retry-check cache message. */
    private static final short RETRY_TYPE = (short)(CoreMessagesProvider.MAX_MESSAGE_ID + 2);

    /** Number of remote destinations to broadcast to (a single marshal must serve all of them). */
    private static final int RMT_CNT = 4;

    /** Counts {@code marshal} invocations of {@link MarshalOnceCheckMessage} across the JVM. */
    private static final AtomicInteger MARSHAL_CNT = new AtomicInteger();

    /** Counts {@code marshal} invocations of {@link RetryCheckMessage} across the JVM. */
    private static final AtomicInteger RETRY_MARSHAL_CNT = new AtomicInteger();

    /** When {@code true}, nodes are configured with the SPI failing the first send of {@link RetryCheckMessage}. */
    private boolean failFirstSend;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(failFirstSend ? new FailFirstSendSpi() : new TestRecordingCommunicationSpi());

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "marshal-once-test";
            }

            @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
                registry.registerExtension(MessageFactoryProvider.class, factory -> {
                    factory.register(TYPE, MarshalOnceCheckMessage::new, new Serializer(), new CountingMarshaller());
                    factory.register(RETRY_TYPE, RetryCheckMessage::new, new RetrySerializer(), new RetryCountingMarshaller());
                });
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testBroadcastMarshalsExactlyOnce() throws Exception {
        startGrids(RMT_CNT + 1);

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(grid(0));

        commSpi.record(MarshalOnceCheckMessage.class);

        List<ClusterNode> rmts = new ArrayList<>(RMT_CNT);

        for (int i = 1; i <= RMT_CNT; i++)
            rmts.add(grid(i).localNode());

        MARSHAL_CNT.set(0);

        grid(0).context().io().sendToGridTopic(rmts, GridTopic.TOPIC_IO_TEST, new MarshalOnceCheckMessage(),
            GridIoPolicy.PUBLIC_POOL);

        assertEquals("Broadcast must be sent to all " + RMT_CNT + " nodes", RMT_CNT, commSpi.recordedMessages(true).size());

        assertEquals("A message broadcast to " + RMT_CNT + " nodes must be marshalled exactly once, not per destination",
            1, MARSHAL_CNT.get());
    }

    /** @throws Exception If failed. */
    @Test
    public void testRetryMarshalsExactlyOnce() throws Exception {
        failFirstSend = true;

        startGrids(2);

        RETRY_MARSHAL_CNT.set(0);

        ClusterNode rmt = grid(0).cluster().node(grid(1).localNode().id());

        grid(0).context().cache().context().io().send(rmt, new RetryCheckMessage(), GridIoPolicy.PUBLIC_POOL);

        FailFirstSendSpi spi = (FailFirstSendSpi)grid(0).configuration().getCommunicationSpi();

        assertTrue("The first send attempt must have failed to exercise the retry path", spi.failed.get());

        assertEquals("A message sent with a retry must be marshalled exactly once, not per attempt",
            1, RETRY_MARSHAL_CNT.get());
    }

    /** Fieldless message; only the registered marshaller's invocation count matters. */
    private static class MarshalOnceCheckMessage implements Message {
        // No fields.
    }

    /** Header-only serializer for the fieldless {@link MarshalOnceCheckMessage}. */
    private static class Serializer implements MessageSerializer<MarshalOnceCheckMessage> {
        /** {@inheritDoc} */
        @Override public boolean writeTo(MarshalOnceCheckMessage msg, MessageWriter writer) {
            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(msg.directType()))
                    return false;

                writer.onHeaderWritten();
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(MarshalOnceCheckMessage msg, MessageReader reader) {
            return true;
        }
    }

    /** Marshaller that only counts {@code marshal} calls — no idempotency guard, so it counts raw invocations. */
    private static class CountingMarshaller implements MessageMarshaller<MarshalOnceCheckMessage> {
        /** {@inheritDoc} */
        @Override public void marshal(MarshalOnceCheckMessage msg, GridKernalContext kctx, CacheObjectContext nested) {
            MARSHAL_CNT.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void unmarshal(MarshalOnceCheckMessage msg, GridKernalContext kctx, CacheObjectContext nested,
            ClassLoader clsLdr) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void unmarshal(MarshalOnceCheckMessage msg, GridKernalContext kctx) {
            // No-op.
        }
    }

    /** Fieldless cache message: takes the {@code GridCacheIoManager} send path with its retry loop. */
    private static class RetryCheckMessage extends GridCacheMessage {
        /** {@inheritDoc} */
        @Override public boolean addDeploymentInfo() {
            return false;
        }
    }

    /** Header-only serializer for the fieldless {@link RetryCheckMessage}. */
    private static class RetrySerializer implements MessageSerializer<RetryCheckMessage> {
        /** {@inheritDoc} */
        @Override public boolean writeTo(RetryCheckMessage msg, MessageWriter writer) {
            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(msg.directType()))
                    return false;

                writer.onHeaderWritten();
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(RetryCheckMessage msg, MessageReader reader) {
            return true;
        }
    }

    /** Marshaller that only counts {@code marshal} calls of {@link RetryCheckMessage}. */
    private static class RetryCountingMarshaller implements MessageMarshaller<RetryCheckMessage> {
        /** {@inheritDoc} */
        @Override public void marshal(RetryCheckMessage msg, GridKernalContext kctx, CacheObjectContext nested) {
            RETRY_MARSHAL_CNT.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void unmarshal(RetryCheckMessage msg, GridKernalContext kctx, CacheObjectContext nested,
            ClassLoader clsLdr) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void unmarshal(RetryCheckMessage msg, GridKernalContext kctx) {
            // No-op.
        }
    }

    /** SPI failing the first send of a {@link RetryCheckMessage} to force one retry iteration. */
    private static class FailFirstSendSpi extends TcpCommunicationSpi {
        /** Set once the single simulated failure has been thrown. */
        private final AtomicBoolean failed = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage && GridTestUtils.getFieldValue(msg, "msg") instanceof RetryCheckMessage
                && failed.compareAndSet(false, true))
                throw new IgniteSpiException("Simulated transient send failure");

            super.sendMessage(node, msg, ackC);
        }
    }
}
