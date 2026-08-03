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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.CustomWireFormMessage;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * A custom wire form is a step of its own: {@link MessageMarshalling} runs it around marshalling, so it happens even
 * for a message that has no marshaller at all.
 */
@SuppressWarnings("deprecation")
public class MessageWireFormStepTest extends GridCommonAbstractTest {
    /** Direct type of the message that has both steps. */
    private static final short BOTH_TYPE = (short)(CoreMessagesProvider.MAX_MESSAGE_ID + 1);

    /** Direct type of the message that has a wire form only. */
    private static final short WIRE_ONLY_TYPE = (short)(CoreMessagesProvider.MAX_MESSAGE_ID + 2);

    /** Steps taken, in the order they happened. */
    private static final List<String> STEPS = Collections.synchronizedList(new ArrayList<>());

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "wire-form-step-test";
            }

            @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
                registry.registerExtension(MessageFactoryProvider.class, factory -> {
                    factory.register(BOTH_TYPE, new BothSerializer(), new RecordingMarshaller());
                    factory.register(WIRE_ONLY_TYPE, new WireOnlySerializer(), null);
                });
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        STEPS.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testWireFormRunsAroundMarshalling() throws Exception {
        GridKernalContext kctx = startGrid(0).context();

        BothMessage msg = new BothMessage();

        MessageMarshalling.marshal(msg, kctx, null);
        MessageMarshalling.unmarshal(msg, kctx, null, getClass().getClassLoader());

        assertEquals("The wire form must be built before marshalling and read back after unmarshalling",
            List.of("toWireForm", "marshal", "unmarshal", "fromWireForm"), STEPS);
    }

    /** @throws Exception If failed. */
    @Test
    public void testWireFormRunsWithoutMarshaller() throws Exception {
        GridKernalContext kctx = startGrid(0).context();

        WireOnlyMessage msg = new WireOnlyMessage();

        MessageMarshalling.marshal(msg, kctx, null);
        MessageMarshalling.unmarshal(msg, kctx, null, getClass().getClassLoader());

        assertEquals("A message with nothing to marshal still gets its wire form step",
            List.of("toWireForm", "fromWireForm"), STEPS);
    }

    /** Fieldless message with both steps; only the order of the recorded steps matters. */
    private static class BothMessage implements MarshallableMessage, CustomWireFormMessage {
        /** {@inheritDoc} */
        @Override public void marshal(Marshaller marsh) {
            STEPS.add("marshal");
        }

        /** {@inheritDoc} */
        @Override public void unmarshal(Marshaller marsh, ClassLoader clsLdr) {
            STEPS.add("unmarshal");
        }

        /** {@inheritDoc} */
        @Override public void toWireForm() {
            STEPS.add("toWireForm");
        }

        /** {@inheritDoc} */
        @Override public void fromWireForm() {
            STEPS.add("fromWireForm");
        }
    }

    /** Fieldless message with a wire form and nothing to marshal, so no marshaller is registered for it. */
    private static class WireOnlyMessage implements CustomWireFormMessage {
        /** {@inheritDoc} */
        @Override public void toWireForm() {
            STEPS.add("toWireForm");
        }

        /** {@inheritDoc} */
        @Override public void fromWireForm() {
            STEPS.add("fromWireForm");
        }
    }

    /** Marshaller that delegates to the message, as a generated one does. */
    private static class RecordingMarshaller implements MessageMarshaller<BothMessage> {
        /** {@inheritDoc} */
        @Override public void marshal(BothMessage msg, GridKernalContext kctx, CacheObjectContext nested) {
            msg.marshal((Marshaller)null);
        }

        /** {@inheritDoc} */
        @Override public void unmarshal(BothMessage msg, GridKernalContext kctx, CacheObjectContext nested,
            ClassLoader clsLdr) {
            msg.unmarshal(null, clsLdr);
        }
    }

    /** Header-only serializer for {@link BothMessage}. */
    private static class BothSerializer implements MessageSerializer<BothMessage> {
        /** {@inheritDoc} */
        @Override public boolean writeTo(BothMessage msg, MessageWriter writer) {
            return writeHeader(msg, writer);
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(BothMessage msg, MessageReader reader) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public BothMessage createMessage() {
            return new BothMessage();
        }
    }

    /** Header-only serializer for {@link WireOnlyMessage}. */
    private static class WireOnlySerializer implements MessageSerializer<WireOnlyMessage> {
        /** {@inheritDoc} */
        @Override public boolean writeTo(WireOnlyMessage msg, MessageWriter writer) {
            return writeHeader(msg, writer);
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(WireOnlyMessage msg, MessageReader reader) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public WireOnlyMessage createMessage() {
            return new WireOnlyMessage();
        }
    }

    /** @return {@code true} if the header is written. */
    private static boolean writeHeader(Message msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        return true;
    }
}
