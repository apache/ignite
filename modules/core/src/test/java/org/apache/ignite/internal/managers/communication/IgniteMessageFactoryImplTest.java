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

import java.nio.ByteBuffer;

import org.apache.ignite.IgniteException;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for default implementation of {@link IgniteMessageFactory} interface.
 */
public class IgniteMessageFactoryImplTest {
    /** Test message 1 type. */
    private static final short TEST_MSG_1_TYPE = 1;

    /** Test message 2 type. */
    private static final short TEST_MSG_2_TYPE = 2;

    /** Unknown message type. */
    private static final short UNKNOWN_MSG_TYPE = 0;

    /**
     * Tests that impossible register new message after initialization.
     */
    @Test(expected = IllegalStateException.class)
    public void testReadOnly() {
        MessageFactory[] factories = {new TestMessageFactoryPovider(), new TestMessageFactory()};

        IgniteMessageFactory msgFactory = new IgniteMessageFactoryImpl(factories);

        msgFactory.register((short)0, () -> null);
    }

    /**
     * Tests that proper message type will be returned by message factory.
     */
    @Test
    public void testCreate() {
        MessageFactory[] factories = {new TestMessageFactoryPovider(), new TestMessageFactory()};

        IgniteMessageFactory msgFactory = new IgniteMessageFactoryImpl(factories);

        Message msg;

        msg = msgFactory.create(TEST_MSG_1_TYPE);
        assertTrue(msg instanceof TestMessage1);

        msg = msgFactory.create(TEST_MSG_2_TYPE);
        assertTrue(msg instanceof TestMessage2);

        msg = msgFactory.create(TEST_MSG_2_TYPE);
        assertTrue(msg instanceof TestMessage2);
    }

    /**
     * Tests that exception will be thrown for unknown message direct type.
     */
    @Test(expected = IgniteException.class)
    public void testCreate_UnknownMessageType() {
        MessageFactory[] factories = {new TestMessageFactoryPovider(), new TestMessageFactory()};

        IgniteMessageFactory msgFactory = new IgniteMessageFactoryImpl(factories);

        msgFactory.create(UNKNOWN_MSG_TYPE);
    }

    /**
     * Tests attemption of registration message with already registered message type.
     */
    @Test(expected = IgniteException.class)
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    public void testRegisterTheSameType() {
        MessageFactory[] factories = {
                new TestMessageFactoryPovider(),
                new TestMessageFactory(),
                new TestMessageFactoryPoviderWithTheSameDirectType()
        };

        new IgniteMessageFactoryImpl(factories);
    }

    /**
     * {@link MessageFactoryProvider} implementation.
     */
    private static class TestMessageFactoryPovider implements MessageFactoryProvider {
        /** {@inheritDoc} */
        @Override public void registerAll(IgniteMessageFactory factory) {
            factory.register(TEST_MSG_1_TYPE, TestMessage1::new);
        }
    }

    /**
     * {@link MessageFactoryProvider} implementation with message direct type which is already registered.
     */
    private static class TestMessageFactoryPoviderWithTheSameDirectType implements MessageFactoryProvider {
        /** {@inheritDoc} */
        @Override public void registerAll(IgniteMessageFactory factory) {
            factory.register(TEST_MSG_1_TYPE, TestMessage1::new);
        }
    }

    /**
     * {@link MessageFactory} implementation whish still uses creation with switch-case.
     */
    private static class TestMessageFactory implements MessageFactory {
        /** {@inheritDoc} */
        @Override public @Nullable Message create(short type) {
            switch (type) {
                case TEST_MSG_2_TYPE:
                    return new TestMessage2();

                default:
                    return null;
            }
        }
    }

    /** Test message. */
    private static class TestMessage1 implements Message {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            // No-op.
        }
    }

    /** Test message. */
    private static class TestMessage2 implements Message {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            return 2;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            // No-op.
        }
    }
}
