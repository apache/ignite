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

import org.apache.ignite.internal.network.serialization.marshal.MarshalException;
import org.apache.ignite.internal.network.serialization.marshal.MarshalledObject;
import org.apache.ignite.internal.network.serialization.marshal.UnmarshalException;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * Serialization service implementation.
 */
public class SerializationService {
    /** Message serialization registry. */
    private final MessageSerializationRegistry messageRegistry;

    /** Descriptor registry. */
    private final ClassDescriptorRegistry descriptorRegistry;

    /** Descriptor factory. */
    private final ClassDescriptorFactory descriptorFactory;

    /** User object marshaller. */
    private final UserObjectMarshaller marshaller;

    /**
     * Constructor.
     *
     * @param messageRegistry Message registry.
     * @param userObjectSerializationContext User object serialization context.
     */
    public SerializationService(
            MessageSerializationRegistry messageRegistry,
            UserObjectSerializationContext userObjectSerializationContext
    ) {
        this.messageRegistry = messageRegistry;
        this.descriptorRegistry = userObjectSerializationContext.descriptorRegistry();
        this.descriptorFactory = userObjectSerializationContext.descriptorFactory();
        this.marshaller = userObjectSerializationContext.marshaller();
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
     * @param object Object to serialize.
     * @param <T> Object's type.
     * @throws UserObjectSerializationException If failed to serialize an object.
     * @see UserObjectMarshaller#marshal(Object, Class)
     */
    public <T> MarshalledObject writeMarshallable(T object) throws UserObjectSerializationException {
        try {
            return marshaller.marshal(object);
        } catch (MarshalException e) {
            throw new UserObjectSerializationException("Failed to serialize object of type " + object.getClass().getName(), e);
        }
    }

    /**
     * Deserializes a marshallable object from a byte array.
     *
     * @param descriptors Descriptors provider.
     * @param array Byte array that contains a serialized object.
     * @param <T> Object's type.
     * @throws UserObjectSerializationException If failed to deserialize an object.
     * @see UserObjectMarshaller#unmarshal(byte[], IdIndexedDescriptors)
     */
    @Nullable
    public <T> T readMarshallable(IdIndexedDescriptors descriptors, byte[] array)
            throws UserObjectSerializationException {
        try {
            return marshaller.unmarshal(array, descriptors);
        } catch (UnmarshalException e) {
            throw new UserObjectSerializationException("Failed to deserialize object: " + e.getMessage(), e);
        }
    }

    /**
     * Gets a class descriptor for the class specified by a name.
     *
     * @param typeName Class' name.
     * @return Class descriptor.
     */
    public ClassDescriptor getOrCreateDescriptor(String typeName) {
        Class<?> clazz;
        try {
            clazz = Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("Class " + typeName + " is not found", e);
        }

        ClassDescriptor descriptor = descriptorRegistry.getDescriptor(clazz);
        if (descriptor != null) {
            return descriptor;
        } else {
            return descriptorFactory.create(clazz);
        }
    }

    public ClassDescriptor getDescriptor(int descriptorId) {
        return descriptorRegistry.getDescriptor(descriptorId);
    }

    public ClassDescriptorRegistry getDescriptorRegistry() {
        return descriptorRegistry;
    }
}
