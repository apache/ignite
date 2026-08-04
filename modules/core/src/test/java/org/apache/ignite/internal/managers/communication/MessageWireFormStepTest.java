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
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWireForm;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.PlainMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * {@link MessageWire} finds the wire form codegen wrote for a message and calls it, both ways. What that wire form
 * does inside — the message's own step, the bytes, the walk — is put there statically, and the codegen tests check
 * the generated source for it.
 */
public class MessageWireFormStepTest extends GridCommonAbstractTest {
    /** Direct type of the message that has both steps. */
    private static final short BOTH_TYPE = (short)(CoreMessagesProvider.MAX_MESSAGE_ID + 1);

    /** Direct type of the message that has a step of its own but no wire form registered. */
    private static final short OWN_STEP_TYPE = (short)(CoreMessagesProvider.MAX_MESSAGE_ID + 2);

    /** Direct type of the message that needs nothing but its serializer. */
    private static final short PLAIN_TYPE = (short)(CoreMessagesProvider.MAX_MESSAGE_ID + 3);

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
                    factory.register(BOTH_TYPE, new BothSerializer(), new RecordingWireForm());
                    factory.register(OWN_STEP_TYPE, new OwnStepSerializer(), null);
                    factory.register(PLAIN_TYPE, new PlainSerializer(), null);
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
    public void testWireFormIsFoundAndCalled() throws Exception {
        GridKernalContext kctx = startGrid(0).context();

        BothMessage msg = new BothMessage();

        MessageWire.toWire(msg, kctx, null);
        MessageWire.fromWire(msg, kctx, null, getClass().getClassLoader());

        assertEquals("The wire form registered for the message must be called both ways",
            List.of("prepare", "restore"), STEPS);
    }

    /** Fieldless {@link PlainMessage} with a step of its own; registered with no wire form. */
    private static class PlainWireFormMessage implements PlainMessage, CustomWireFormMessage {
        /** {@inheritDoc} */
        @Override public void toWireForm() {
            STEPS.add("toWireForm");
        }

        /** {@inheritDoc} */
        @Override public void fromWireForm() {
            STEPS.add("fromWireForm");
        }
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

    /** Fieldless message with a step of its own and nothing else; registered with no wire form. */
    private static class OwnStepMessage implements CustomWireFormMessage {
        /** {@inheritDoc} */
        @Override public void toWireForm() {
            STEPS.add("toWireForm");
        }

        /** {@inheritDoc} */
        @Override public void fromWireForm() {
            STEPS.add("fromWireForm");
        }
    }

    /** Wire form that records both calls. */
    private static class RecordingWireForm implements MessageWireForm<BothMessage> {
        /** {@inheritDoc} */
        @Override public void prepare(BothMessage msg, GridKernalContext kctx, CacheObjectContext nested) {
            STEPS.add("prepare");
        }

        /** {@inheritDoc} */
        @Override public void restore(BothMessage msg, GridKernalContext kctx, CacheObjectContext nested,
            ClassLoader clsLdr) {
            STEPS.add("restore");
        }
    }

    /** Header-only serializer for {@link PlainWireFormMessage}. */
    private static class PlainSerializer implements MessageSerializer<PlainWireFormMessage> {
        /** {@inheritDoc} */
        @Override public boolean writeTo(PlainWireFormMessage msg, MessageWriter writer) {
            return writeHeader(msg, writer);
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(PlainWireFormMessage msg, MessageReader reader) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public PlainWireFormMessage createMessage() {
            return new PlainWireFormMessage();
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

    /** Header-only serializer for {@link OwnStepMessage}. */
    private static class OwnStepSerializer implements MessageSerializer<OwnStepMessage> {
        /** {@inheritDoc} */
        @Override public boolean writeTo(OwnStepMessage msg, MessageWriter writer) {
            return writeHeader(msg, writer);
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(OwnStepMessage msg, MessageReader reader) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public OwnStepMessage createMessage() {
            return new OwnStepMessage();
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
