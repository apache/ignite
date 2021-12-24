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

package org.apache.ignite.internal.network.serialization;

import java.util.Map;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;

/**
 * Serialization service implementation.
 */
public class SerializationService {
    /** Message serialization registry. */
    private final MessageSerializationRegistry messageRegistry;

    /** User object serializer. */
    private final UserObjectSerializer userObjectSerializer;

    public SerializationService(MessageSerializationRegistry messageRegistry, UserObjectSerializer userObjectSerializer) {
        this.messageRegistry = messageRegistry;
        this.userObjectSerializer = userObjectSerializer;
    }

    /**
     * Creates a message serializer.
     *
     * @see MessageSerializationRegistry#createSerializer(short, short)
     */
    public <T extends NetworkMessage> MessageSerializer<T> createSerializer(short groupType, short messageType) {
        return messageRegistry.createSerializer(groupType, messageType);
    }

    /**
     * Creates a message deserializer.
     *
     * @see MessageSerializationRegistry#createDeserializer(short, short)
     */
    public <T extends NetworkMessage> MessageDeserializer<T> createDeserializer(short groupType, short messageType) {
        return messageRegistry.createDeserializer(groupType, messageType);
    }

    /**
     * Serializes a marshallable object to a byte array.
     *
     * @see UserObjectSerializer#write(Object)
     */
    public <T> SerializationResult writeMarshallable(T object) {
        return userObjectSerializer.write(object);
    }

    /**
     * Deserializes a marshallable object from a byte array.
     *
     * @see UserObjectSerializer#read(Map, byte[])
     */
    public <T> T readMarshallable(Map<Integer, ClassDescriptor> descriptor, byte[] array) {
        return userObjectSerializer.read(descriptor, array);
    }

    /**
     * Returns {@code true} if type descriptor id belongs to the range reserved for built-in types, {@code false} otherwise.
     *
     * @param typeDescriptorId Type descriptor id.
     * @return {@code true} if descriptor belongs to the range reserved for built-in types, {@code false} otherwise.
     */
    public boolean shouldBeBuiltIn(int typeDescriptorId) {
        return userObjectSerializer.shouldBeBuiltIn(typeDescriptorId);
    }

    /**
     * Gets a class descriptor by the descriptor id.
     *
     * @param typeDescriptorId Type descriptor id.
     * @return Class descriptor.
     */
    public ClassDescriptor getClassDescriptor(int typeDescriptorId) {
        return userObjectSerializer.getClassDescriptor(typeDescriptorId);
    }

    /**
     * Gets a class descriptor for the class specified by a name.
     *
     * @param typeName Class' name.
     * @return Class descriptor.
     */
    public ClassDescriptor getClassDescriptor(String typeName) {
        return userObjectSerializer.getClassDescriptor(typeName);
    }
}
