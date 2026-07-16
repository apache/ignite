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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.communication.UnknownMessageException;

/**
 * Base type for all messages sent between nodes, both over the communication SPI and via discovery.
 * <p>
 * Serialized fields are declared by annotating instance fields; {@ignitelink org.apache.ignite.internal.MessageProcessor} then
 * generates the serializer, so implementations should not hand-write {@code writeTo}/{@code readFrom}. Available
 * field annotations (see each annotation's javadoc for details):
 * <ul>
 *     <li>{@ignitelink org.apache.ignite.internal.Order @Order} — an ordered serialized field (the basic building block);</li>
 *     <li>{@ignitelink org.apache.ignite.internal.Compress @Compress} — compress the field's serialized form;</li>
 *     <li>{@ignitelink org.apache.ignite.internal.NioField @NioField} — a low-level NIO field;</li>
 *     <li>{@ignitelink org.apache.ignite.internal.CustomMapper @CustomMapper} — map the enum field via a custom mapper;</li>
 *     <li>{@ignitelink org.apache.ignite.internal.Marshalled @Marshalled} — for
 *         {@ignitelink org.apache.ignite.internal.MarshallableMessage MarshallableMessage} payloads serialized via a
 *         {@link org.apache.ignite.marshaller.Marshaller}; the flavour (single blob, per-element messages or blobs,
 *         map) is derived from the shape of the companion wire field(s) it names.</li>
 * </ul>
 */
public interface Message {
    /** Direct type size in bytes. */
    int DIRECT_TYPE_SIZE = 2;

    /** Registry of message class to direct type mappings, populated during factory initialization. */
    Map<Class<? extends Message>, Short> REGISTRATIONS = new ConcurrentHashMap<>();

    /** Per-class cache over {@link #REGISTRATIONS}; keeps {@link #directType()} off the map lookup done for every sent message. */
    ClassValue<Short> DIRECT_TYPES = new ClassValue<>() {
        @Override protected Short computeValue(Class<?> type) {
            Short directType = REGISTRATIONS.get(type);

            if (directType == null)
                throw new UnknownMessageException(type.asSubclass(Message.class));

            return directType;
        }
    };

    /**
     * Gets message type.
     *
     * @return Message type.
     */
    default short directType() {
        return DIRECT_TYPES.get(getClass());
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

        if ((type != null) && (type != directType))
            throw new IgniteException(clazz.getSimpleName() + " is already registered for direct type " + type);
    }
}
