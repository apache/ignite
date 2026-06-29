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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Verifies the marshal-once-before-fan-out contract of the collection {@code send}: a message broadcast to N remote
 * nodes is marshalled <b>exactly once</b> (prepared before the fan-out and reused for every destination), not once per
 * destination. A counting marshaller is registered for a test message; broadcasting it to {@link #RMT_CNT} remote nodes
 * must produce {@code RMT_CNT} sends but a single {@code prepareMarshal}. Counting the sends keeps the check honest (a
 * lone send would also marshal once). The unmarshal-once counterpart lives in {@link MessageFinishUnmarshalOnceTest}.
 */
public class MessageMarshalOnceTest extends GridCommonAbstractTest {
    /** Direct type for the test message, past the core range. */
    private static final short TYPE = (short)(CoreMessagesProvider.MAX_MESSAGE_ID + 1);

    /** Number of remote destinations to broadcast to (a single marshal must serve all of them). */
    private static final int RMT_CNT = 4;

    /** Counts {@code prepareMarshal} invocations of {@link MarshalOnceCheckMessage} across the JVM. */
    private static final AtomicInteger MARSHAL_CNT = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "marshal-once-test";
            }

            @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
                registry.registerExtension(MessageFactoryProvider.class, factory ->
                    factory.register(TYPE, MarshalOnceCheckMessage::new, new Serializer(), new CountingMarshaller()));
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

    /** Marshaller that only counts {@code prepareMarshal} calls — no idempotency guard, so it counts raw invocations. */
    private static class CountingMarshaller implements MessageMarshaller<MarshalOnceCheckMessage> {
        /** {@inheritDoc} */
        @Override public void prepareMarshal(MarshalOnceCheckMessage msg, GridKernalContext kctx, CacheObjectContext nested) {
            MARSHAL_CNT.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void finishUnmarshal(MarshalOnceCheckMessage msg, GridKernalContext kctx, CacheObjectContext nested,
            ClassLoader clsLdr) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void finishUnmarshal(MarshalOnceCheckMessage msg, GridKernalContext kctx) {
            // No-op.
        }
    }
}
