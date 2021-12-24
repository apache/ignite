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

/** User object serializer. */
public interface UserObjectSerializer {
    /**
     * Reads a marshallable object from a byte array using a map of descriptors.
     *
     * @param descriptors Descriptor map.
     * @param array       Byte array.
     * @param <T>         Object type.
     * @return Unmarshalled object.
     */
    <T> T read(Map<Integer, ClassDescriptor> descriptors, byte[] array);

    /**
     * Marshalls object.
     *
     * @param object Object.
     * @param <T> Object's type.
     * @return {@link SerializationResult}.
     */
    <T> SerializationResult write(T object);

    ClassDescriptor getClassDescriptor(int typeDescriptorId);

    ClassDescriptor getClassDescriptor(String typeName);

    boolean shouldBeBuiltIn(int typeDescriptorId);
}
