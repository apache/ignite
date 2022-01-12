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

/**
 * Describes how a class is to be serialized.
 */
public class Serialization {
    /** Serialization type. */
    private final SerializationType type;

    /** Whether a Serializable has writeObject() + readObject() methods. */
    private final boolean hasSerializationOverride;
    /** Whether a Serializable has readObjectNoData() method. */
    private final boolean hasReadObjectNoData;
    /** Whether a Serializable/Externalizable has writeReplace() method. */
    private final boolean hasWriteReplace;
    /** Whether a Serializable/Externalizable has readResolve() method. */
    private final boolean hasReadResolve;

    /**
     * Creates a new Serialization.
     *
     * @param type                     type
     * @param hasSerializationOverride whether a Serializable has writeObject() + readObject() methods
     * @param hasReadObjectNoData      whether a Serializable has readObjectNoData() method
     * @param hasWriteReplace          whether a Serializable/Externalizable has writeReplace() method
     * @param hasReadResolve           whether a Serializable/Externalizable has readResolve() method
     */
    public Serialization(
            SerializationType type,
            boolean hasSerializationOverride,
            boolean hasReadObjectNoData,
            boolean hasWriteReplace,
            boolean hasReadResolve
    ) {
        assert type == SerializationType.SERIALIZABLE
                || (type == SerializationType.EXTERNALIZABLE && !hasSerializationOverride && !hasReadObjectNoData)
                || (!hasSerializationOverride && !hasWriteReplace && !hasReadResolve);

        this.type = type;
        this.hasSerializationOverride = hasSerializationOverride;
        this.hasReadObjectNoData = hasReadObjectNoData;
        this.hasWriteReplace = hasWriteReplace;
        this.hasReadResolve = hasReadResolve;
    }

    /**
     * Creates a new Serialization with all optional features disabled.
     *
     * @param type serialization type
     */
    public Serialization(SerializationType type) {
        this(type, false, false, false, false);
    }

    /**
     * Returns serialization type.
     *
     * @return serialization type
     */
    public SerializationType type() {
        return type;
    }

    /**
     * Returns whether serialization override (writeObject() + readObject()) is present.
     *
     * @return whether serialization override (writeObject() + readObject()) is present
     */
    public boolean hasSerializationOverride() {
        return hasSerializationOverride;
    }

    /**
     * Returns whether readObjectNoData() method is present.
     *
     * @return whether readObjectNoData() method is present
     */
    public boolean hasReadObjectNoData() {
        return hasReadObjectNoData;
    }

    /**
     * Returns whether writeReplace() method is present.
     *
     * @return whether writeReplace() method is present
     */
    public boolean hasWriteReplace() {
        return hasWriteReplace;
    }

    /**
     * Returns whether readResolve() method is present.
     *
     * @return whether readResolve() method is present
     */
    public boolean hasReadResolve() {
        return hasReadResolve;
    }
}
