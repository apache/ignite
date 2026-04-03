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

package org.apache.ignite.plugin.extensions.communication;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;

/**
 * Base class for all communication messages.
 */
public interface Message {
    /** Direct type size in bytes. */
    int DIRECT_TYPE_SIZE = 2;

    /** Registry of message class to direct type mappings, populated during factory initialization. */
    Map<Class<?>, Short> REGISTRATIONS = new ConcurrentHashMap<>();

    /**
     * Writes this message to provided byte buffer.
     *
     * @param buf Byte buffer.
     * @param writer Writer.
     * @return Whether message was fully written.
     * @deprecated Use the code-generated {@code MessageSerializer} instead.
     */
    @Deprecated
    default boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads this message from provided byte buffer.
     *
     * @param buf Byte buffer.
     * @param reader Reader.
     * @return Whether message was fully read.
     * @deprecated Use the code-generated {@code MessageSerializer} instead.
     */
    @Deprecated
    default boolean readFrom(ByteBuffer buf, MessageReader reader) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets message type.
     *
     * @return Message type.
     */
    /**
     * Gets message type.
     *
     * @return Message type.
     */
    default short directType() {
        var clazz = getClass();
        Short type = REGISTRATIONS.get(clazz);

        if (type == null) {
            throw new IgniteException("No registration for class " + clazz.getSimpleName());
        }

        return type;
    }

    /**
     * Registers the direct type for this message class. Called during message factory initialization
     * to populate the {@link #REGISTRATIONS} map so that {@link #directType()} can resolve types
     * without requiring each message class to override it.
     *
     * @param directType Direct type to register.
     * @throws IgniteException If this message class is already registered with a different direct type.
     */
    default void registerAsDirectType(short directType) {
        var clazz = getClass();
        var type = REGISTRATIONS.putIfAbsent(clazz, directType);

        if ((type != null) && (type != directType)) {
            throw new IgniteException(clazz.getSimpleName() + " is already registered for direct type " + type);
        }
    }
}
