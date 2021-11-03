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

package org.apache.ignite.network.serialization;

import org.apache.ignite.network.NetworkConfigurationException;
import org.apache.ignite.network.NetworkMessage;

/**
 * Container that maps message types to {@link MessageSerializationFactory} instances.
 */
public interface MessageSerializationRegistry {
    /**
     * Registers message serialization factory by message type.
     *
     * @param groupType   Message group type.
     * @param messageType Message type.
     * @param factory     Message's serialization factory.
     * @return This registry.
     * @throws NetworkConfigurationException If there is an already registered factory for the given type.
     */
    MessageSerializationRegistry registerFactory(
            short groupType, short messageType, MessageSerializationFactory<?> factory
    );

    /**
     * Creates a {@link MessageSerializer} for the given message type.
     *
     * <p>{@link MessageSerializationRegistry} does not track the correspondence between the message type and its Java representation,
     * so the actual generic specialization of the returned provider relies on the caller of this method.
     *
     * @param <T>         Type of a message.
     * @param groupType   Group type of a message.
     * @param messageType Message type.
     * @return Message's serializer.
     */
    <T extends NetworkMessage> MessageSerializer<T> createSerializer(short groupType, short messageType);

    /**
     * Creates a {@link MessageDeserializer} for the given message type.
     *
     * <p>{@link MessageSerializationRegistry} does not track the correspondence between the message type and its Java representation,
     * so the actual generic specialization of the returned provider relies on the caller of this method.
     *
     * @param <T>         Type of a message.
     * @param groupType   Group type of a message.
     * @param messageType Message type.
     * @return Message's deserializer.
     */
    <T extends NetworkMessage> MessageDeserializer<T> createDeserializer(short groupType, short messageType);
}
