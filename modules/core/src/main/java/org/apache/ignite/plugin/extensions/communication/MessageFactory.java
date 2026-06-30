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

import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheMessageDeployer;
import org.jetbrains.annotations.Nullable;

/**
 * Message factory for all communication messages registered using {@link #register(short, Supplier, MessageSerializer)} method call.
 */
public interface MessageFactory {
    /**
     * Register message factory with given direct type. All messages must be registered during construction
     * of class which implements this interface. Any invocation of this method after initialization is done must
     * throw {@link IllegalStateException} exception.
     *
     * @param directType Direct type.
     * @param supplier Message factory.
     * @param serializer Message serializer.
     * @throws IgniteException In case of attempt to register message with direct type which is already registered.
     * @throws IllegalStateException On any invocation of this method when class which implements this interface
     * is alredy constructed.
     */
    public void register(short directType, Supplier<Message> supplier, MessageSerializer serializer) throws IgniteException;

    /**
     * Register message factory with given direct type and serializer. The direct type is also registered
     * on the message class via {@link Message#registerAsDirectType(short)} so that {@link Message#directType()}
     * resolves automatically without requiring each message to override it.
     *
     * <p>This is the preferred registration method. All messages must be registered during construction
     * of class which implements this interface. Any invocation of this method after initialization is done must
     * throw {@link IllegalStateException} exception.
     *
     * @param directType Direct type.
     * @param supplier Message factory.
     * @param serializer Message serializer.
     * @throws IgniteException In case of attempt to register message with direct type which is already registered.
     * @throws IllegalStateException On any invocation of this method when class which implements this interface
     * is already constructed.
     */
    default void register(int directType, Supplier<Message> supplier, MessageSerializer serializer) throws IgniteException {
        register((short)directType, supplier, serializer);
    }

    /**
     * Creates new message instance of provided type.
     *
     * @param type Message type.
     * @return Message instance.
     */
    public Message create(short type);

    /**
     * Register message factory with given direct type, serializer, and marshaller. All messages must be registered
     * during construction of class which implements this interface.
     *
     * @param directType Direct type.
     * @param supplier Message factory.
     * @param serializer Message serializer.
     * @param marshaller Message marshaller, or {@code null} for non-marshallable messages.
     * @throws IgniteException In case of attempt to register message with direct type which is already registered.
     */
    default void register(short directType, Supplier<Message> supplier, MessageSerializer serializer,
        @Nullable MessageMarshaller marshaller) throws IgniteException {
        register(directType, supplier, serializer);
    }

    /**
     * Register message factory with given direct type, serializer, marshaller, and deployer. All messages must be
     * registered during construction of class which implements this interface.
     *
     * @param directType Direct type.
     * @param supplier Message factory.
     * @param serializer Message serializer.
     * @param marshaller Message marshaller, or {@code null} for non-marshallable messages.
     * @param deployer Message deployer, or {@code null} for messages without deployable fields.
     * @throws IgniteException In case of attempt to register message with direct type which is already registered.
     */
    default void register(short directType, Supplier<Message> supplier, MessageSerializer serializer,
        @Nullable MessageMarshaller marshaller, @Nullable GridCacheMessageDeployer deployer) throws IgniteException {
        register(directType, supplier, serializer, marshaller);
    }

    /**
     * Returns {@code MessageSerializer} for provided type.
     *
     * @param type Message type.
     * @return Message serializer.
     */
    public MessageSerializer serializer(short type);

    /**
     * Returns {@code MessageMarshaller} for provided type, or {@code null} if none is registered
     * (e.g. for {@code NonMarshallableMessage} types).
     *
     * @param type Message type.
     * @return Message marshaller, or {@code null}.
     */
    public @Nullable MessageMarshaller marshaller(short type);

    /**
     * Returns {@code GridCacheMessageDeployer} for provided type, or {@code null} if none is registered
     * (e.g. for messages without deployable fields).
     *
     * @param type Message type.
     * @return Message deployer, or {@code null}.
     */
    default @Nullable GridCacheMessageDeployer deployer(short type) {
        return null;
    }
}
