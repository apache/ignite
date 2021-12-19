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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.Nullable;

/**
 * Class descriptor factory context.
 */
public class ClassDescriptorFactoryContext {
    /** Quantity of descriptor ids reserved for the default descriptors. */
    private static final int DEFAULT_DESCRIPTORS_OFFSET_COUNT = 1000;

    /** Sequential id generator for class descriptors. */
    private final AtomicInteger idGenerator = new AtomicInteger(DEFAULT_DESCRIPTORS_OFFSET_COUNT);

    /** Map class -> descriptor id. */
    private final ConcurrentMap<Class<?>, Integer> idMap = new ConcurrentHashMap<>();

    /** Map descriptor id -> class descriptor. */
    private final ConcurrentMap<Integer, ClassDescriptor> descriptorMap = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public ClassDescriptorFactoryContext() {
        for (BuiltinType value : BuiltinType.values()) {
            addPredefinedDescriptor(value.clazz(), value.asClassDescriptor());
        }
    }

    /**
     * Adds predefined class descriptor with a statically configured id.
     *
     * @param clazz Class.
     * @param descriptor Descriptor.
     */
    private void addPredefinedDescriptor(Class<?> clazz, ClassDescriptor descriptor) {
        int descriptorId = descriptor.descriptorId();

        Integer existingId = idMap.put(clazz, descriptorId);

        assert existingId == null;

        ClassDescriptor existingDescriptor = descriptorMap.put(descriptorId, descriptor);

        assert existingDescriptor == null;
    }

    /**
     * Gets descriptor id for the class.
     *
     * @param clazz Class.
     * @return Descriptor id.
     */
    public int getId(Class<?> clazz) {
        return idMap.computeIfAbsent(clazz, unused -> idGenerator.getAndIncrement());
    }

    /**
     * Gets a descriptor by the id.
     *
     * @param descriptorId Descriptor id.
     * @return Descriptor.
     */
    @Nullable
    public ClassDescriptor getDescriptor(int descriptorId) {
        return descriptorMap.get(descriptorId);
    }

    /**
     * Gets a descriptor by the class.
     *
     * @param clazz Class.
     * @return Descriptor.
     */
    @Nullable
    public ClassDescriptor getDescriptor(Class<?> clazz) {
        Integer descriptorId = idMap.get(clazz);

        if (descriptorId == null) {
            return null;
        }

        return descriptorMap.get(descriptorId);
    }

    /**
     * Returns a descriptor by its ID or throws an exception if the ID is not known.
     *
     * @param descriptorId ID by which to obtain a descriptor
     * @return descriptor
     */
    public ClassDescriptor getRequiredDescriptor(int descriptorId) {
        ClassDescriptor descriptor = getDescriptor(descriptorId);
        if (descriptor == null) {
            throw new IllegalStateException("No descriptor exists with ID=" + descriptorId);
        }
        return descriptor;
    }

    /**
     * Gets a descriptor by the class or throws an exception if no such class is known.
     *
     * @param clazz Class.
     * @return Descriptor.
     */
    public ClassDescriptor getRequiredDescriptor(Class<?> clazz) {
        ClassDescriptor descriptor = getDescriptor(clazz);
        if (descriptor == null) {
            throw new IllegalStateException("No descriptor exists for " + clazz);
        }
        return descriptor;
    }

    /**
     * Returns a descriptor for a built-in type.
     *
     * @param builtinType   built-in type for lookup
     */
    public ClassDescriptor getBuiltInDescriptor(BuiltinType builtinType) {
        return getRequiredDescriptor(builtinType.descriptorId());
    }

    /**
     * Returns a descriptor for {@code null} value.
     *
     * @return a descriptor for {@code null} value
     */
    public ClassDescriptor getNullDescriptor() {
        return getDescriptor(Null.class);
    }

    /**
     * Returns a descriptor for {@link Enum} built-in type.
     *
     * @return a descriptor for {@link Enum} built-in type
     */
    public ClassDescriptor getEnumDescriptor() {
        return getDescriptor(Enum.class);
    }

    /**
     * Returns {@code true} if there is a descriptor for the id.
     *
     * @param descriptorId Descriptor id.
     * @return {@code true} if there is a descriptor for the id.
     */
    public boolean hasDescriptor(int descriptorId) {
        return descriptorMap.containsKey(descriptorId);
    }

    /**
     * Adds a descriptor.
     *
     * @param descriptor Descriptor.
     */
    public void addDescriptor(ClassDescriptor descriptor) {
        Integer descriptorId = idMap.get(descriptor.clazz());

        assert descriptorId != null : "Attempting to store an unregistered descriptor";

        int realDescriptorId = descriptor.descriptorId();

        assert descriptorId == realDescriptorId : "Descriptor id doesn't match, registered=" + descriptorId + ", real="
            + realDescriptorId;

        descriptorMap.put(realDescriptorId, descriptor);
    }

    /**
     * Returns {@code true} if descriptor with the specified descriptor id is built-in, {@code false} otherwise.
     *
     *
     * @param descriptorId Descriptor id.
     * @return Whether descriptor is built-in.
     */
    public static boolean isBuiltIn(int descriptorId) {
        return descriptorId < DEFAULT_DESCRIPTORS_OFFSET_COUNT;
    }
}
