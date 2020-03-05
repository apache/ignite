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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.ignite.IgniteException;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.jetbrains.annotations.Nullable;

/**
 * Message factory implementation which is responsible for instantiation of all communication messages.
 */
public class IgniteMessageFactoryImpl implements IgniteMessageFactory {
    /** Offset. */
    private static final int OFF = -Short.MIN_VALUE;

    /** Array size. */
    private static final int ARR_SIZE = 1 << Short.SIZE;

    /** Message suppliers. */
    private final Supplier<Message>[] msgSuppliers = (Supplier<Message>[]) Array.newInstance(Supplier.class, ARR_SIZE);

    /** Initialized flag. If {@code true} then new message type couldn't be registered. */
    private boolean initialized;

    /**
     * Contructor.
     *
     * @param factories Concrete message factories or message factory providers. Cfn't be empty or {@code null}.
     */
    public IgniteMessageFactoryImpl(MessageFactory[] factories) {
        if (factories == null || factories.length == 0)
            throw new IllegalArgumentException("Message factory couldn't be initialized. Factories aren't provided.");

        List<MessageFactory> old = new ArrayList<>(factories.length);

        for (MessageFactory factory : factories) {
            if (factory instanceof MessageFactoryProvider) {
                MessageFactoryProvider p = (MessageFactoryProvider)factory;

                p.registerAll(this);
            }
            else
                old.add(factory);
        }

        if (!old.isEmpty()) {
            for (int i = 0; i < ARR_SIZE; i++) {
                Supplier<Message> curr = msgSuppliers[i];

                if (curr == null) {
                    short directType = indexToDirectType(i);

                    for (MessageFactory factory : old) {
                        Message msg = factory.create(directType);

                        if (msg != null)
                            register(directType, () -> factory.create(directType));
                    }
                }
            }
        }

        initialized = true;
    }

    /** {@inheritDoc} */
    @Override public void register(short directType, Supplier<Message> supplier) throws IgniteException {
        if (initialized) {
            throw new IllegalStateException("Message factory is already initialized. " +
                    "Registration of new message types is forbidden.");
        }

        int idx = directTypeToIndex(directType);

        Supplier<Message> curr = msgSuppliers[idx];

        if (curr == null)
            msgSuppliers[idx] = supplier;
        else
            throw new IgniteException("Message factory is already registered for direct type: " + directType);
    }

    /**
     * Creates new message instance of provided direct type.
     * <p>
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
