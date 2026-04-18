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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for default implementation of {@link CoreMessagesProvider} interface.
 */
public class IgniteMessageFactoryImplTest {
    /** Test message 1 type. */
    private static final short TEST_MSG_1_TYPE = 1;

    /** Test message 2 type. */
    private static final short TEST_MSG_2_TYPE = 2;

    /** Test message 42 type. */
    private static final short TEST_MSG_42_TYPE = 42;

    /** Unknown message type. */
    private static final short UNKNOWN_MSG_TYPE = 0;

    /**
     * Tests that impossible register new message after initialization.
     */
    @Test(expected = IllegalStateException.class)
    public void testReadOnly() {
        MessageFactoryProvider[] factories = {new TestMessageFactoryProvider(), new AdditionalTestMessageFactoryProvider()};

        MessageFactory msgFactory = new IgniteMessageFactoryImpl(factories);

        msgFactory.register((short)0, () -> null);
    }

    /**
     * Tests that proper message type will be returned by message factory.
     */
    @Test
    public void testCreate() {
        MessageFactoryProvider[] factories = {new TestMessageFactoryProvider(), new AdditionalTestMessageFactoryProvider()};

        IgniteMessageFactoryImpl msgFactory = new IgniteMessageFactoryImpl(factories);

        Message msg;

        msg = msgFactory.create(TEST_MSG_1_TYPE);
        assertTrue(msg instanceof IgniteMessageFactoryImplTestMessage1);

        msg = msgFactory.create(TEST_MSG_2_TYPE);
        assertTrue(msg instanceof IgniteMessageFactoryImplTestMessage2);

        msg = msgFactory.create(TEST_MSG_42_TYPE);
        assertTrue(msg instanceof IgniteMessageFactoryImplTestMessage42);

        short[] directTypes = msgFactory.registeredDirectTypes();

        assertArrayEquals(directTypes, new short[] {TEST_MSG_1_TYPE, TEST_MSG_2_TYPE, TEST_MSG_42_TYPE});
    }

    /**
     * Tests that exception will be thrown for unknown message direct type.
     */
    @Test(expected = IgniteException.class)
    public void testCreate_UnknownMessageType() {
        MessageFactoryProvider[] factories = {new TestMessageFactoryProvider(), new AdditionalTestMessageFactoryProvider()};

        MessageFactory msgFactory = new IgniteMessageFactoryImpl(factories);

        msgFactory.create(UNKNOWN_MSG_TYPE);
    }

    /**
     * Tests attemption of registration message with already registered message type.
     */
    @Test(expected = IgniteException.class)
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    public void testRegisterTheSameType() {
        MessageFactoryProvider[] factories = {
            new TestMessageFactoryProvider(),
            new AdditionalTestMessageFactoryProvider(),
            new TestMessageFactoryProviderWithTheSameDirectType()
        };

        new IgniteMessageFactoryImpl(factories);
    }

    /**
     * {@link MessageFactoryProvider} implementation.
     */
    private static class TestMessageFactoryProvider implements MessageFactoryProvider {
        /** {@inheritDoc} */
        @Override public void registerAll(MessageFactory factory) {
            factory.register(TEST_MSG_1_TYPE, IgniteMessageFactoryImplTestMessage1::new,
                new IgniteMessageFactoryImplTestMessage1Serializer());
            factory.register(TEST_MSG_42_TYPE, IgniteMessageFactoryImplTestMessage42::new,
                new IgniteMessageFactoryImplTestMessage42Serializer());
        }
    }

    /**
     * {@link MessageFactoryProvider} implementation with message direct type which is already registered.
     */
    private static class TestMessageFactoryProviderWithTheSameDirectType implements MessageFactoryProvider {
        /** {@inheritDoc} */
        @Override public void registerAll(MessageFactory factory) {
            factory.register(TEST_MSG_1_TYPE, IgniteMessageFactoryImplTestMessage1::new,
                new IgniteMessageFactoryImplTestMessage1Serializer());
        }
    }

    /**
     * Additional {@link MessageFactoryProvider} implementation.
     */
    private static class AdditionalTestMessageFactoryProvider implements MessageFactoryProvider {
        /** {@inheritDoc} */
        @Override public void registerAll(MessageFactory factory) {
            factory.register(TEST_MSG_2_TYPE, IgniteMessageFactoryImplTestMessage2::new,
                new IgniteMessageFactoryImplTestMessage2Serializer());
        }
    }
}
