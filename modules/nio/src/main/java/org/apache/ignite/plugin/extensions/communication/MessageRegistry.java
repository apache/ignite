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
 * Registry of message class to direct type mappings behind {@link Message#directType()}, populated during factory
 * initialization via {@link Message#registerAsDirectType(short)}. Kept package-private so the map is mutable only
 * through the validated registration path.
 */
final class MessageRegistry {
    /** Message class to direct type mappings. */
    private static final Map<Class<? extends Message>, Short> REGISTRATIONS = new ConcurrentHashMap<>();

    /** Per-class cache over {@link #REGISTRATIONS}; keeps {@link #directType} off the map lookup done for every sent message. */
    private static final ClassValue<Short> DIRECT_TYPES = new ClassValue<>() {
        @Override protected Short computeValue(Class<?> type) {
            Short directType = REGISTRATIONS.get(type);

            if (directType == null)
                throw new UnknownMessageException(type.asSubclass(Message.class));

            return directType;
        }
    };

    /** */
    private MessageRegistry() {
        // No-op.
    }

    /**
     * @param cls Message class.
     * @return Direct type registered for {@code cls}.
     * @throws UnknownMessageException If {@code cls} is not registered.
     */
    static short directType(Class<? extends Message> cls) {
        return DIRECT_TYPES.get(cls);
    }

    /**
     * Registers the direct type for {@code cls}.
     *
     * @param cls Message class.
     * @param directType Direct type to register.
     * @throws IgniteException If {@code cls} is already registered with a different direct type.
     */
    static void register(Class<? extends Message> cls, short directType) {
        Short type = REGISTRATIONS.putIfAbsent(cls, directType);

        if ((type != null) && (type != directType))
            throw new IgniteException(cls.getSimpleName() + " is already registered for direct type " + type);
    }
}
