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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;

/**
 * Represents a marshalled object: the marshalled representation with information about how it was marshalled
 * (including what descriptors were used when marshalling it).
 */
public class MarshalledObject {
    /** Marshalled object representation. */
    private final byte[] bytes;

    /** The descriptors that were used while marshalling the object. */
    private final List<ClassDescriptor> usedDescriptors;

    /**
     * Creates a new {@link MarshalledObject}.
     *
     * @param bytes           marshalled representation bytes
     * @param usedDescriptors the descriptors that were used to marshal the object
     */
    public MarshalledObject(byte[] bytes, List<ClassDescriptor> usedDescriptors) {
        Objects.requireNonNull(bytes, "bytes is null");
        Objects.requireNonNull(usedDescriptors, "usedDescriptors is null");

        this.bytes = Arrays.copyOf(bytes, bytes.length);
        this.usedDescriptors = List.copyOf(usedDescriptors);
    }

    /**
     * Returns marshalled object representation.
     *
     * @return marshalled object representation
     */
    public byte[] bytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    /**
     * Returns the descriptors that were used while marshalling the object.
     *
     * @return the descriptors that were used while marshalling the object
     */
    public List<ClassDescriptor> usedDescriptors() {
        return usedDescriptors;
    }
}
