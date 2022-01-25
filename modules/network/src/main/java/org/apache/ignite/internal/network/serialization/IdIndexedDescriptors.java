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

import org.jetbrains.annotations.Nullable;

/**
 * Container of {@link ClassDescriptor}s indexed by their IDs.
 */
public interface IdIndexedDescriptors {
    /**
     * Returns a descriptor by ID or throws an exception if no such descriptor is known.
     *
     * @param descriptorId ID of the descriptor
     * @return descriptor by ID
     */
    @Nullable
    ClassDescriptor getDescriptor(int descriptorId);

    /**
     * Returns a descriptor by ID or throws an exception if no such descriptor is known.
     *
     * @param descriptorId ID of the descriptor
     * @return descriptor by ID
     */
    default ClassDescriptor getRequiredDescriptor(int descriptorId) {
        ClassDescriptor descriptor = getDescriptor(descriptorId);

        if (descriptor == null) {
            throw new IllegalStateException("Did not find a descriptor with ID=" + descriptorId);
        }

        return descriptor;
    }

    /**
     * Returns {@code true} if there is a descriptor for the id.
     *
     * @param descriptorId Descriptor id.
     * @return {@code true} if there is a descriptor for the id.
     */
    default boolean hasDescriptor(int descriptorId) {
        return getDescriptor(descriptorId) != null;
    }

    /**
     * Returns a descriptor for a built-in type.
     *
     * @param builtinType   built-in type for lookup
     */
    default ClassDescriptor getBuiltInDescriptor(BuiltInType builtinType) {
        return getRequiredDescriptor(builtinType.descriptorId());
    }

    /**
     * Returns a descriptor for {@link java.lang.reflect.Proxy} built-in type.
     *
     * @return a descriptor for {@link java.lang.reflect.Proxy} built-in type
     */
    default ClassDescriptor getProxyDescriptor() {
        return getBuiltInDescriptor(BuiltInType.PROXY);
    }
}
