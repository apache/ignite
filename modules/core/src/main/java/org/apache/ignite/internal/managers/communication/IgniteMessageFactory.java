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

package org.apache.ignite.internal.managers.communication;

import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMessageDeployer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * Internal extension of {@link MessageFactory} that also registers and resolves the marshal and deployment
 * companions of a message. Kept out of the public factory interface: marshallers and deployers are bound to
 * kernal and cache internals ({@code GridKernalContext}, {@code GridCacheSharedContext}) the public SPI module
 * must not depend on.
 *
 * @see MessageMarshaller
 * @see GridCacheMessageDeployer
 */
public interface IgniteMessageFactory<M extends Message, CM extends GridCacheMessage> extends MessageFactory<M> {
    /** {@inheritDoc} */
    @Override default void register(short directType, Supplier<M> supplier, MessageSerializer<M> serializer)
        throws IgniteException {
        register(directType, supplier, serializer, null, null);
    }

    /**
     * Registers a message with the given direct type, serializer, and marshaller. All messages must be registered
     * during construction of the class that implements this interface.
     *
     * @param directType Direct type ({@link Message#directType()}) to register the message under.
     * @param supplier Message supplier.
     * @param serializer Message serializer.
     * @param marshaller Message marshaller, or {@code null} for non-marshallable messages.
     * @throws IgniteException If a message is already registered under the given direct type.
     */
    default void register(short directType, Supplier<M> supplier, MessageSerializer<M> serializer,
        @Nullable MessageMarshaller<M> marshaller) throws IgniteException {
        register(directType, supplier, serializer, marshaller, null);
    }

    /**
     * Registers a message with the given direct type, serializer, marshaller, and deployer. All messages must be
     * registered during construction of the class that implements this interface.
     *
     * @param directType Direct type ({@link Message#directType()}) to register the message under.
     * @param supplier Message supplier.
     * @param serializer Message serializer.
     * @param marshaller Message marshaller, or {@code null} for non-marshallable messages.
     * @param deployer Message deployer, or {@code null} for messages without deployable fields.
     * @throws IgniteException If a message is already registered under the given direct type.
     */
    public void register(short directType, Supplier<M> supplier, MessageSerializer<M> serializer,
        @Nullable MessageMarshaller<M> marshaller, @Nullable GridCacheMessageDeployer<CM> deployer) throws IgniteException;

    /**
     * Returns {@code MessageMarshaller} for provided type, or {@code null} if none is registered
     * (e.g. for {@code NonMarshallableMessage} types).
     *
     * @param type Message type.
     * @return Message marshaller, or {@code null}.
     */
    public @Nullable MessageMarshaller<M> marshaller(short type);

    /**
     * Returns {@code GridCacheMessageDeployer} for provided type, or {@code null} if none is registered
     * (e.g. for messages without deployable fields).
     *
     * @param type Message type.
     * @return Message deployer, or {@code null}.
     */
    public @Nullable GridCacheMessageDeployer<CM> deployer(short type);
}
