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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;
import org.apache.ignite.internal.network.message.FieldDescriptorMessage;
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
    private final ConcurrentMap<Integer, ClassDescriptor> mergedDescriptorMap = new ConcurrentHashMap<>();

    /**
     * Immutable view over {@link #mergedDescriptorMap}. Used by {@link #serializationService}.
     */
    private final Map<Integer, ClassDescriptor> descriptorMapView = Collections.unmodifiableMap(mergedDescriptorMap);

    public PerSessionSerializationService(@NotNull SerializationService serializationService) {
        this.serializationService = serializationService;
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
     * @see SerializationService#writeMarshallable(Object)
     */
    public <T> SerializationResult writeMarshallable(T marshallable) {
        return serializationService.writeMarshallable(marshallable);
    }

    /**
     * Deserializes a marshallable object from a byte array.
     *
     * @see SerializationService#readMarshallable(Map, byte[])
     */
    public <T> T readMarshallable(List<ClassDescriptorMessage> missingDescriptors, byte[] marshallableData) {
        mergeDescriptors(missingDescriptors);

        return serializationService.readMarshallable(descriptorMapView, marshallableData);
    }

    /**
     * Creates a list of messages holding class descriptors.
     *
     * @param descriptorIds Ids of class descriptors.
     * @return List of class descriptor network messages.
     */
    public List<ClassDescriptorMessage> createClassDescriptorsMessages(List<Integer> descriptorIds) {
        return descriptorIds.stream().map(id -> {
            ClassDescriptor descriptor = serializationService.getClassDescriptor(id);

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
                    .hasSerializationOverride(serialization.hasSerializationOverride())
                    .hasReadObjectNoData(serialization.hasReadObjectNoData())
                    .hasWriteReplace(serialization.hasWriteReplace())
                    .hasReadResolve(serialization.hasReadResolve())
                    .descriptorId(descriptor.descriptorId())
                    .className(descriptor.className())
                    .superClassDescriptorId(superClassDescriptorIdForMessage(descriptor))
                    .superClassName(descriptor.superClassName())
                    .build();
        }).collect(toList());
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

            boolean isClsBuiltin = serializationService.shouldBeBuiltIn(clsDescriptorId);

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
        ClassDescriptor localDescriptor = serializationService.getClassDescriptor(clsMsg.className());

        List<FieldDescriptor> remoteFields = clsMsg.fields().stream()
                .map(fieldMsg -> fieldDescriptorFromMessage(fieldMsg, localDescriptor.clazz()))
                .collect(toList());

        SerializationType serializationType = SerializationType.getByValue(clsMsg.serializationType());

        var serialization = new Serialization(
                serializationType,
                clsMsg.hasSerializationOverride(),
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
        return new FieldDescriptor(fieldMsg.name(), getClass(typeDescriptorId, fieldMsg.className()), typeDescriptorId, declaringClass);
    }

    private ClassDescriptor mergeDescriptor(ClassDescriptor localDescriptor, ClassDescriptor remoteDescriptor) {
        // TODO: IGNITE-15948 Handle class structure changes
        return remoteDescriptor;
    }

    private ClassDescriptor getClassDescriptor(int descriptorId, String typeName) {
        if (serializationService.shouldBeBuiltIn(descriptorId)) {
            return serializationService.getClassDescriptor(descriptorId);
        } else {
            return serializationService.getClassDescriptor(typeName);
        }
    }

    private Class<?> getClass(int descriptorId, String typeName) {
        return getClassDescriptor(descriptorId, typeName).getClass();
    }

    @TestOnly
    Map<Integer, ClassDescriptor> getDescriptorMapView() {
        return descriptorMapView;
    }
}
