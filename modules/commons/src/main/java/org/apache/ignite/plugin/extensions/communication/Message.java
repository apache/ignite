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
 * Base class for all communication messages.
 * <p>
 * Wire fields are declared by annotating instance fields; {@link org.apache.ignite.internal.MessageProcessor} then
 * generates the serializer, so implementations should not hand-write {@code writeTo}/{@code readFrom}. Available
 * field annotations (see each annotation's javadoc for details):
 * <ul>
 *     <li>{@link org.apache.ignite.internal.Order @Order} — an ordered wire field (the basic building block);</li>
 *     <li>{@link org.apache.ignite.internal.Compress @Compress} — compress the field's wire form;</li>
 *     <li>{@link org.apache.ignite.internal.NioField @NioField} — a low-level NIO field;</li>
 *     <li>{@link org.apache.ignite.internal.CustomMapper @CustomMapper} — map the field via a custom mapper;</li>
 *     <li>{@link org.apache.ignite.internal.Marshalled @Marshalled} /
 *         {@link org.apache.ignite.internal.MarshalledMap @MarshalledMap} /
 *         {@link org.apache.ignite.internal.MarshalledCollection @MarshalledCollection} /
 *         {@link org.apache.ignite.internal.MarshalledObjects @MarshalledObjects} — for {@link MarshallableMessage}
 *         payloads serialized via a {@link org.apache.ignite.marshaller.Marshaller}.</li>
 * </ul>
 */
public interface Message {
    /** Direct type size in bytes. */
    int DIRECT_TYPE_SIZE = 2;

    /** Registry of message class to direct type mappings, populated during factory initialization. */
    Map<Class<?>, Short> REGISTRATIONS = new ConcurrentHashMap<>();

    /** Per-class cache over {@link #REGISTRATIONS}; keeps {@link #directType()} off the hash lookup on the hot path. */
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
