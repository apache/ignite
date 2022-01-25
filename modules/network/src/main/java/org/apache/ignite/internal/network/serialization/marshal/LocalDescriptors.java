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

package org.apache.ignite.internal.network.serialization.marshal;

import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * Represents class descriptors for classes loaded by the local node.
 */
class LocalDescriptors {
    private final ClassDescriptorRegistry localRegistry;
    private final ClassDescriptorFactory descriptorFactory;

    LocalDescriptors(ClassDescriptorRegistry localRegistry, ClassDescriptorFactory descriptorFactory) {
        this.localRegistry = localRegistry;
        this.descriptorFactory = descriptorFactory;
    }

    ClassDescriptor getOrCreateDescriptor(@Nullable Object object, @Nullable Class<?> declaredClass) {
        if (object == null) {
            return localRegistry.getNullDescriptor();
        }

        // For primitives, we need to keep the declaredClass (it differs from object.getClass()).
        Class<?> classToQueryForOriginalDescriptor = declaredClass != null && declaredClass.isPrimitive()
                ? declaredClass : object.getClass();

        return getOrCreateDescriptor(classToQueryForOriginalDescriptor);
    }

    ClassDescriptor getOrCreateDescriptor(Class<?> objectClass) {
        ClassDescriptor descriptor = DescriptorResolver.resolveDescriptor(objectClass, localRegistry);
        if (descriptor != null) {
            return descriptor;
        }

        Class<?> normalizedClass = DescriptorResolver.normalizeClass(objectClass);

        return descriptorFactory.create(normalizedClass);
    }
}
