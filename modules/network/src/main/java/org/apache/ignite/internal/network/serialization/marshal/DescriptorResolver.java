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
import org.apache.ignite.internal.network.serialization.Classes;
import org.apache.ignite.internal.network.serialization.DescriptorRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * Encapsulates the logic of resolving a class to a descriptor which includes 'normalization' of the resolved class
 * (for example, replacing any concrete enum classes with {@link Enum}).
 */
class DescriptorResolver {
    /**
     * Resolves a descriptor by the given class. If it's a built-in, returns the built-in. If it's custom, searches among
     * the already known descriptors for the exact class. If not found there, returns {@code null}.
     *
     * @param objectClass   class for which to obtain a descriptor
     * @param descriptors   descriptor registry where to search for descriptors of custom classes
     * @return corresponding descriptor or @{code null} if nothing is found
     */
    @Nullable
    static ClassDescriptor resolveDescriptor(Class<?> objectClass, DescriptorRegistry descriptors) {
        if (ProxyMarshaller.isProxyClass(objectClass)) {
            return descriptors.getProxyDescriptor();
        }

        Class<?> normalizedClass = normalizeClass(objectClass);

        return descriptors.getDescriptor(normalizedClass);
    }

    static Class<?> normalizeClass(Class<?> objectClass) {
        if (Classes.isRuntimeEnum(objectClass)) {
            return Classes.enumClassAsInSourceCode(objectClass);
        } else {
            return objectClass;
        }
    }

    private DescriptorResolver() {
    }
}
