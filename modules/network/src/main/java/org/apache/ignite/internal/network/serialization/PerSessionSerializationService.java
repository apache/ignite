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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry.shouldBeBuiltIn;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;
import org.apache.ignite.internal.network.message.FieldDescriptorMessage;
import org.apache.ignite.internal.network.serialization.marshal.MarshalledObject;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Per-session serialization service.
 * Handles (de-)serialization of messages, object (de-)serialization and class descriptor merging.
 */
public class PerSessionSerializationService {
    /** Network messages factory. */
    private static final NetworkMessagesFactory MSG_FACTORY = new NetworkMessagesFactory();

    /** Integer value that is sent when there is no descriptor. */
    private static final int NO_DESCRIPTOR_ID = Integer.MIN_VALUE;

    /** Global serialization service. */
    @NotNull
    private final SerializationService serializationService;

    /**
     * Map with merged class descriptors. They are the result of the merging of a local and a remote descriptor.
     * The key in this map is a <b>remote</b> descriptor id.
     */
    private final Int2ObjectMap<ClassDescriptor> mergedDescriptorMap = new Int2ObjectOpenHashMap<>();

    /**
     * A collection of the descriptors that were sent to the remote node.
     */
    private final IntSet sentDescriptors = new IntOpenHashSet();

    /**
     * Descriptors provider.
     */
    private final CompositeIdIndexedDescriptors descriptors;

    /**
     * Constructor.
     *
     * @param serializationService Serialization service.
     */
    public PerSessionSerializationService(@NotNull SerializationService serializationService) {
        this.serializationService = serializationService;
        this.descriptors = new CompositeIdIndexedDescriptors(new MapBackedIdIndexedDescriptors(mergedDescriptorMap),
                serializationService.getDescriptorRegistry());
    }

    /**
     * Creates a message serializer.
     *
     * @see SerializationService#createSerializer(short, short)
     */
    public <T extends NetworkMessage> MessageSerializer<T> createMessageSerializer(short groupType, short messageType) {
        return serializationService.createSerializer(groupType, messageType);
    }

    /**
     * Creates a message deserializer.
     *
     * @see SerializationService#createDeserializer(short, short)
     */
    public <T extends NetworkMessage> MessageDeserializer<T> createMessageDeserializer(short groupType, short messageType) {
        return serializationService.createDeserializer(groupType, messageType);
    }

    /**
     * Serializes a marshallable object to a byte array.
     *
     * @param marshallable Marshallable object to serialize.
     * @param <T> Object's type.
     * @throws UserObjectSerializationException If failed to serialize an object.
     * @see SerializationService#writeMarshallable(Object)
     */
    public <T> MarshalledObject writeMarshallable(T marshallable)
            throws UserObjectSerializationException {
        return serializationService.writeMarshallable(marshallable);
    }

    /**
     * Deserializes a marshallable object from a byte array.
     *
     * @param missingDescriptors Descriptors that were received from the remote node.
     * @param array Byte array that contains a serialized object.
     * @param <T> Object's type.
     * @throws UserObjectSerializationException If failed to deserialize an object.
     * @see SerializationService#readMarshallable(IdIndexedDescriptors, byte[])
     */
    public <T> T readMarshallable(List<ClassDescriptorMessage> missingDescriptors, byte[] array)
            throws UserObjectSerializationException {
        mergeDescriptors(missingDescriptors);

        return serializationService.readMarshallable(descriptors, array);
    }

    /**
     * Creates a list of messages holding class descriptors.
     *
     * @param descriptors Class descriptors.
     * @return List of class descriptor network messages.
     */
    @Nullable
    public List<ClassDescriptorMessage> createClassDescriptorsMessages(Set<ClassDescriptor> descriptors) {
        List<ClassDescriptorMessage> messages = descriptors.stream()
                .filter(descriptor -> {
                    int descriptorId = descriptor.descriptorId();
                    return !sentDescriptors.contains(descriptorId) && !shouldBeBuiltIn(descriptorId);
                })
                .map(descriptor -> {
                    List<FieldDescriptorMessage> fields = descriptor.fields().stream()
                            .map(d -> {
                                return MSG_FACTORY.fieldDescriptorMessage()
                                    .name(d.name())
                                    .typeDescriptorId(d.typeDescriptorId())
                                    .className(d.clazz().getName())
                                    .build();
                            })
                            .collect(toList());

                    Serialization serialization = descriptor.serialization();

                    return MSG_FACTORY.classDescriptorMessage()
                            .fields(fields)
                            .isFinal(descriptor.isFinal())
                            .serializationType(serialization.type().value())
                            .hasWriteObject(serialization.hasWriteObject())
                    .hasReadObject(serialization.hasReadObject())
                            .hasReadObjectNoData(serialization.hasReadObjectNoData())
                            .hasWriteReplace(serialization.hasWriteReplace())
                            .hasReadResolve(serialization.hasReadResolve())
                            .descriptorId(descriptor.descriptorId())
                            .className(descriptor.className())
                            .superClassDescriptorId(superClassDescriptorIdForMessage(descriptor))
                            .superClassName(descriptor.superClassName())
                            .build();
                }).collect(toList());

        messages.forEach(classDescriptorMessage -> sentDescriptors.add(classDescriptorMessage.descriptorId()));

        return messages;
    }

    private int superClassDescriptorIdForMessage(ClassDescriptor descriptor) {
        Integer id = descriptor.superClassDescriptorId();

        if (id == null) {
            return NO_DESCRIPTOR_ID;
        }

        return id;
    }

    private void mergeDescriptors(List<ClassDescriptorMessage> remoteDescriptors) {
        for (ClassDescriptorMessage clsMsg : remoteDescriptors) {
            int clsDescriptorId = clsMsg.descriptorId();

            boolean isClsBuiltin = shouldBeBuiltIn(clsDescriptorId);

            if (isClsBuiltin) {
                continue;
            }

            if (mergedDescriptorMap.containsKey(clsDescriptorId)) {
                continue;
            }

            mergedDescriptorMap.put(clsDescriptorId, messageToMergedClassDescriptor(clsMsg));
        }
    }

    /**
     * Converts {@link ClassDescriptorMessage} to a {@link ClassDescriptor} and merges it with a local {@link ClassDescriptor} of the
     * same class.
     *
     * @param clsMsg ClassDescriptorMessage.
     * @return Merged class descriptor.
     */
    @NotNull
    private ClassDescriptor messageToMergedClassDescriptor(ClassDescriptorMessage clsMsg) {
        ClassDescriptor localDescriptor = serializationService.getOrCreateDescriptor(clsMsg.className());

        List<FieldDescriptor> remoteFields = clsMsg.fields().stream()
                .map(fieldMsg -> fieldDescriptorFromMessage(fieldMsg, localDescriptor.clazz()))
                .collect(toList());

        SerializationType serializationType = SerializationType.getByValue(clsMsg.serializationType());

        var serialization = new Serialization(
                serializationType,
                clsMsg.hasWriteObject(),
                clsMsg.hasReadObject(),
                clsMsg.hasReadObjectNoData(),
                clsMsg.hasWriteReplace(),
                clsMsg.hasReadResolve()
        );

        ClassDescriptor remoteDescriptor = new ClassDescriptor(
                localDescriptor.clazz(),
                clsMsg.descriptorId(),
                superClassDescriptor(clsMsg),
                remoteFields,
                serialization
        );

        return mergeDescriptor(localDescriptor, remoteDescriptor);
    }

    @Nullable
    private ClassDescriptor superClassDescriptor(ClassDescriptorMessage clsMsg) {
        if (clsMsg.superClassDescriptorId() == NO_DESCRIPTOR_ID) {
            return null;
        }
        return getClassDescriptor(clsMsg.superClassDescriptorId(), clsMsg.superClassName());
    }

    private FieldDescriptor fieldDescriptorFromMessage(FieldDescriptorMessage fieldMsg, Class<?> declaringClass) {
        int typeDescriptorId = fieldMsg.typeDescriptorId();
        return new FieldDescriptor(
                fieldMsg.name(),
                getClass(typeDescriptorId, fieldMsg.className()),
                typeDescriptorId,
                fieldMsg.unshared(),
                declaringClass
        );
    }

    private ClassDescriptor mergeDescriptor(ClassDescriptor localDescriptor, ClassDescriptor remoteDescriptor) {
        // TODO: IGNITE-15948 Handle class structure changes
        return remoteDescriptor;
    }

    private ClassDescriptor getClassDescriptor(int descriptorId, String typeName) {
        if (shouldBeBuiltIn(descriptorId)) {
            return serializationService.getDescriptor(descriptorId);
        } else {
            return serializationService.getOrCreateDescriptor(typeName);
        }
    }

    private Class<?> getClass(int descriptorId, String typeName) {
        return getClassDescriptor(descriptorId, typeName).clazz();
    }

    @TestOnly
    Map<Integer, ClassDescriptor> getDescriptorMapView() {
        return mergedDescriptorMap;
    }
}
