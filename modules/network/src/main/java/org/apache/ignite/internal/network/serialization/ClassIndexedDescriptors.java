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
 * Container of {@link ClassDescriptor}s indexed by their classes.
 */
public interface ClassIndexedDescriptors {
    /**
     * Returns a descriptor by class or throws an exception if no such descriptor is known.
     *
     * @param clazz  for lookup
     * @return descriptor by class
     */
    @Nullable
    ClassDescriptor getDescriptor(Class<?> clazz);

    /**
     * Returns a descriptor by class or throws an exception if no such descriptor is known.
     *
     * @param clazz  for lookup
     * @return descriptor by class
     */
    default ClassDescriptor getRequiredDescriptor(Class<?> clazz) {
        ClassDescriptor descriptor = getDescriptor(clazz);

        if (descriptor == null) {
            throw new IllegalStateException("Did not find a descriptor by class=" + clazz);
        }

        return descriptor;
    }

    /**
     * Returns {@code true} if there is a descriptor for the given class.
     *
     * @param clazz  for lookup
     * @return {@code true} if there is a descriptor for the given class
     */
    default boolean hasDescriptor(Class<?> clazz) {
        return getDescriptor(clazz) != null;
    }
}
