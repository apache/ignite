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

import org.apache.ignite.lang.IgniteInternalException;

/**
 * Serialization type.
 */
public enum SerializationType {
    /** Used for predefined descriptors like primitive (or boxed int). See {@link BuiltinType}. */
    BUILTIN(0),
    /** Type for classes that are neither serializable nor externalizable.  */
    ARBITRARY(1),
    /** Externalizable. */
    EXTERNALIZABLE(2),
    /** Serializable (but not Externalizable). */
    SERIALIZABLE(3);

    private final int value;

    SerializationType(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    /**
     * Gets SerializationType by {@link #value}.
     *
     * @param value Value.
     * @return Serialization type.
     */
    public static SerializationType getByValue(int value) {
        for (SerializationType serializationType : SerializationType.values()) {
            if (serializationType.value == value) {
                return serializationType;
            }
        }

        throw new IgniteInternalException("SerializationType by value=" + value + " not found");
    }
}
