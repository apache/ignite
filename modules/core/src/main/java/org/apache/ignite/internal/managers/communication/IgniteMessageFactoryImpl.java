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

import java.lang.reflect.Array;
import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheMessageDeployer;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * Message factory implementation which is responsible for instantiation of all communication messages.
 */
public class IgniteMessageFactoryImpl implements MessageFactory {
    /** Offset. */
    private static final int OFF = -Short.MIN_VALUE;

    /** Array size. */
    private static final int ARR_SIZE = 1 << Short.SIZE;

    /** Message suppliers. */
    private final Supplier<Message>[] msgSuppliers = (Supplier<Message>[])Array.newInstance(Supplier.class, ARR_SIZE);

    /** Message serializers. */
    private final MessageSerializer[] msgSerializers = (MessageSerializer[])Array.newInstance(MessageSerializer.class, ARR_SIZE);

    /** Message marshallers (null entry = no marshaller registered for that type). */
    private final MessageMarshaller[] msgMarshallers = (MessageMarshaller[])Array.newInstance(MessageMarshaller.class, ARR_SIZE);

    /** Message deployers (null entry = no deployer registered for that type). */
    private final GridCacheMessageDeployer[] msgDeployers =
        (GridCacheMessageDeployer[])Array.newInstance(GridCacheMessageDeployer.class, ARR_SIZE);

    /** Initialized flag. If {@code true} then new message type couldn't be registered. */
    private boolean initialized;

    /** Min index of registered message supplier. */
    private int minIdx = Integer.MAX_VALUE;

    /** Max index of registered message supplier. */
    private int maxIdx = -1;

    /** Count of registered message suppliers. */
    private int cnt;

    /**
     * Contructor.
     *
     * @param factories Concrete message factories or message factory providers. Cfn't be empty or {@code null}.
     */
    public IgniteMessageFactoryImpl(MessageFactoryProvider[] factories) {
        if (factories == null || factories.length == 0)
            throw new IllegalArgumentException("Message factory couldn't be initialized. Factories aren't provided.");

        for (MessageFactoryProvider factory : factories)
            factory.registerAll(this);

        initialized = true;
    }

    /**
     * Registers a message type with a serializer, an optional marshaller, and an optional deployer.
     *
     * @param directType  Direct type.
     * @param supplier    Message factory.
     * @param serializer  Message serializer.
     * @param marshaller  Message marshaller, or {@code null} for NonMarshallableMessage types.
     * @param deployer    Message deployer, or {@code null} for messages without deployable fields.
     */
    @Override public void register(short directType, Supplier<Message> supplier, MessageSerializer serializer,
        @Nullable MessageMarshaller marshaller, @Nullable GridCacheMessageDeployer deployer) throws IgniteException {
        if (initialized) {
            throw new IllegalStateException("Message factory is already initialized. " +
                    "Registration of new message types is forbidden.");
        }

        try {
            Message msg = supplier.get();

            if (marshaller == null && msg instanceof MarshallableMessage) {
                throw new IgniteException("Message implements MarshallableMessage but is registered without" +
                    " a marshaller, so it would be sent unmarshalled [directType=" + directType +
                    ", cls=" + msg.getClass().getName() + ']');
            }

            msg.registerAsDirectType(directType);
        }
        catch (NoClassDefFoundError | ExceptionInInitializerError e) {
            // Optional dependency not available (e.g. JTS for GridH2Geometry).
            // Registration will succeed when used in an environment with the dependency.
        }

        int idx = directTypeToIndex(directType);

        Supplier<Message> curr = msgSuppliers[idx];

        if (curr == null) {
            msgSuppliers[idx] = supplier;
            msgSerializers[idx] = serializer;
            msgMarshallers[idx] = marshaller;
            msgDeployers[idx] = deployer;

            minIdx = Math.min(idx, minIdx);

            maxIdx = Math.max(idx, maxIdx);

            cnt++;
        }
        else
            throw new IgniteException("Message factory is already registered for direct type: " + directType);
    }

    /**
     * Creates new message instance of provided direct type.
     *
     * @param directType Message direct type.
     * @return Message instance.
     * @throws IgniteException If there are no any message factory for given {@code directType}.
     */
    @Override public @Nullable Message create(short directType) {
        Supplier<Message> supplier = msgSuppliers[directTypeToIndex(directType)];

        if (supplier == null)
            throw new UnknownMessageException(directType);

        return supplier.get();
    }

    /** {@inheritDoc} */
    @Override public MessageSerializer serializer(short directType) {
        MessageSerializer serializer = msgSerializers[directTypeToIndex(directType)];

        if (serializer == null)
            throw new IgniteException("Message serializer not found for a message type: " + directType);

        return serializer;
    }

    /** {@inheritDoc} */
    @Override public @Nullable MessageMarshaller marshaller(short directType) {
        return msgMarshallers[directTypeToIndex(directType)];
    }

    /** {@inheritDoc} */
    @Override public @Nullable GridCacheMessageDeployer deployer(short directType) {
        return msgDeployers[directTypeToIndex(directType)];
    }

    /**
     * Returns direct types of all registered messages.
     *
     * @return Direct types of all registered messages.
     */
    public short[] registeredDirectTypes() {
        short[] res = new short[cnt];

        if (cnt > 0) {
            for (int i = minIdx, p = 0; i <= maxIdx; i++) {
                if (msgSuppliers[i] != null)
                    res[p++] = indexToDirectType(i);
            }
        }

        return res;
    }

    /**
     * @param directType Direct type.
     */
    private static int directTypeToIndex(short directType) {
        return directType + OFF;
    }

    /**
     * @param idx Index.
     */
    private static short indexToDirectType(int idx) {
        int res = idx - OFF;

        assert res >= Short.MIN_VALUE && res <= Short.MAX_VALUE;

        return (short)res;
    }
}
