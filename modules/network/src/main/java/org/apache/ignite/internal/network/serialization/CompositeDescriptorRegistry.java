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
 * Descriptor provider that uses {@link ClassDescriptorRegistry} for built-in descriptor ids and
 * delegates to another {@link IdIndexedDescriptors} for other ids; when looking descriptors by class, it first
 * consults another {@link ClassIndexedDescriptors} and only then resorts to the {@link ClassDescriptorRegistry} instance.
 */
public class CompositeDescriptorRegistry implements DescriptorRegistry {
    private final IdIndexedDescriptors idIndexedDescriptors;
    private final ClassIndexedDescriptors classIndexedDescriptors;
    private final ClassDescriptorRegistry ctx;

    /**
     * Constructor.
     */
    public CompositeDescriptorRegistry(
            IdIndexedDescriptors idIndexedDescriptors,
            ClassIndexedDescriptors classIndexedDescriptors,
            ClassDescriptorRegistry ctx
    ) {
        this.idIndexedDescriptors = idIndexedDescriptors;
        this.classIndexedDescriptors = classIndexedDescriptors;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ClassDescriptor getDescriptor(int descriptorId) {
        if (ClassDescriptorRegistry.shouldBeBuiltIn(descriptorId)) {
            return ctx.getDescriptor(descriptorId);
        }

        return idIndexedDescriptors.getDescriptor(descriptorId);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ClassDescriptor getDescriptor(Class<?> clazz) {
        if (classIndexedDescriptors.hasDescriptor(clazz)) {
            return classIndexedDescriptors.getDescriptor(clazz);
        }

        return ctx.getDescriptor(clazz);
    }
}
