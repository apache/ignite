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
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.apache.ignite.IgniteException;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Message factory implementation which is responsible for instantiation of all communication messages.
 */
public class IgniteMessageFactoryImpl implements MessageFactory {
    /** Offset. */
    private static final int OFF = -Short.MIN_VALUE;

    /** Array size. */
    private static final int ARR_SIZE = 1 << Short.SIZE;

    /** Delegate serialization to {@code Message} methods. */
    private static final MessageSerializer DEFAULT_SERIALIZER = new MessageSerializer() {
        /** {@inheritDoc} */
        @Override public boolean writeTo(Message msg, ByteBuffer buf, MessageWriter writer) {
            return msg.writeTo(buf, writer);
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(Message msg, ByteBuffer buf, MessageReader reader) {
            return msg.readFrom(buf, reader);
        }
    };

    /** Message suppliers. */
    private final Supplier<Message>[] msgSuppliers = (Supplier<Message>[])Array.newInstance(Supplier.class, ARR_SIZE);

    /** Message serializers. */
    private final MessageSerializer[] msgSerializers = (MessageSerializer[])Array.newInstance(MessageSerializer.class, ARR_SIZE);

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

    /** {@inheritDoc} */
    @Override public void register(short directType, Supplier<Message> supplier, MessageSerializer serializer) throws IgniteException {
        if (initialized) {
            throw new IllegalStateException("Message factory is already initialized. " +
                    "Registration of new message types is forbidden.");
        }

        int idx = directTypeToIndex(directType);

        Supplier<Message> curr = msgSuppliers[idx];

        if (curr == null) {
            msgSuppliers[idx] = supplier;
            msgSerializers[idx] = serializer;

            minIdx = Math.min(idx, minIdx);

            maxIdx = Math.max(idx, maxIdx);

            cnt++;
        }
        else
            throw new IgniteException("Message factory is already registered for direct type: " + directType);
    }

    /** {@inheritDoc} */
    @Override public void register(short directType, Supplier<Message> supplier) throws IgniteException {
        register(directType, supplier, DEFAULT_SERIALIZER);
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
            throw new IgniteException("Invalid message type: " + directType);

        return supplier.get();
    }

    /**
     * @param directType Message direct type.
     * @return Message instance.
     * @throws IgniteException If there are no any message factory for given {@code directType}.
     */
    @Override public MessageSerializer serializer(short directType) {
        MessageSerializer serializer = msgSerializers[directTypeToIndex(directType)];

        if (serializer == null)
            throw new IgniteException("Invalid message type: " + directType);

        return serializer;
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
