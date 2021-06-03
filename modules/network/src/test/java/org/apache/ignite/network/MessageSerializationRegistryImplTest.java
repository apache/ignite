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

package org.apache.ignite.network;

import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * {@link MessageSerializationRegistryImpl} tests.
 */
public class MessageSerializationRegistryImplTest {
    /**
     * Tests that a serialization factory can be registered.
     */
    @Test
    public void testRegisterFactory() {
        var registry = new MessageSerializationRegistryImpl();

        registry.registerFactory(Msg.GROUP_TYPE, Msg.TYPE, new MsgSerializationFactory());
    }

    /**
     * Tests that a serialization factory can't be registered if there is an already registered serialization factory
     * with the same direct type.
     */
    @Test
    public void testRegisterFactoryWithSameType() {
        var registry = new MessageSerializationRegistryImpl();

        registry.registerFactory(Msg.GROUP_TYPE, Msg.TYPE, new MsgSerializationFactory());

        assertThrows(
            NetworkConfigurationException.class,
            () -> registry.registerFactory(Msg.GROUP_TYPE, Msg.TYPE, new MsgSerializationFactory())
        );
    }

    /**
     * Tests that it is possible to register serialization factories for the same message types but for different
     * modules.
     */
    @Test
    public void testRegisterFactoryWithSameTypeDifferentModule() {
        var registry = new MessageSerializationRegistryImpl();

        registry.registerFactory(Msg.GROUP_TYPE, Msg.TYPE, new MsgSerializationFactory());

        short nextGroupType = Msg.GROUP_TYPE + 1;

        registry.registerFactory(nextGroupType, Msg.TYPE, new MsgSerializationFactory());

        assertNotNull(registry.createDeserializer(Msg.GROUP_TYPE, Msg.TYPE));
        assertNotNull(registry.createDeserializer(nextGroupType, Msg.TYPE));
    }

    /**
     * Tests that a {@link MessageSerializer} and a {@link MessageDeserializer} can be created if a
     * {@link MessageSerializationFactory} was registered.
     */
    @Test
    public void testCreateSerializers() {
        var registry = new MessageSerializationRegistryImpl();

        registry.registerFactory(Msg.GROUP_TYPE, Msg.TYPE, new MsgSerializationFactory());

        assertNotNull(registry.createSerializer(Msg.GROUP_TYPE, Msg.TYPE));
        assertNotNull(registry.createDeserializer(Msg.GROUP_TYPE, Msg.TYPE));
    }

    /**
     * Tests that creation of a {@link MessageSerializer} or a {@link MessageDeserializer} fails if a
     * {@link MessageSerializationFactory} was not registered.
     */
    @Test
    public void testCreateSerializersIfNotRegistered() {
        var registry = new MessageSerializationRegistryImpl();

        assertThrows(AssertionError.class, () -> registry.createSerializer(Msg.GROUP_TYPE, Msg.TYPE));
        assertThrows(AssertionError.class, () -> registry.createDeserializer(Msg.GROUP_TYPE, Msg.TYPE));
    }

    /** */
    private static class Msg implements NetworkMessage {
        /** */
        static final short GROUP_TYPE = 0;

        /** */
        static final short TYPE = 0;

        /** {@inheritDoc} */
        @Override public short messageType() {
            return TYPE;
        }

        /** {@inheritDoc} */
        @Override public short groupType() {
            return GROUP_TYPE;
        }
    }

    /** */
    private static class MsgSerializationFactory implements MessageSerializationFactory<Msg> {
        /** {@inheritDoc} */
        @Override public MessageDeserializer<Msg> createDeserializer() {
            return mock(MessageDeserializer.class);
        }

        /** {@inheritDoc} */
        @Override public MessageSerializer<Msg> createSerializer() {
            return mock(MessageSerializer.class);
        }
    }
}
