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

/**
 * Message factory for all communication messages registered using {@link #register(short, Supplier)} method call.
 */
public interface MessageFactory {
    /**
     * Register message factory with given direct type. All messages must be registered during construction
     * of class which implements this interface. Any invocation of this method after initialization is done must
     * throw {@link IllegalStateException} exception.
     *
     * @param directType Direct type.
     * @param supplier Message factory.
     * @throws IgniteException In case of attempt to register message with direct type which is already registered.
     * @throws IllegalStateException On any invocation of this method when class which implements this interface
     * is alredy constructed.
     */
    public void register(short directType, Supplier<Message> supplier) throws IgniteException;

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
     * Register message factory using the direct type obtained from the serializer via {@link MessageSerializer#directType()}.
     * This is a convenience method equivalent to {@link #register(short, Supplier, MessageSerializer)} where the direct
     * type is derived from the serializer itself.
     *
     * <p>All messages must be registered during construction of class which implements this interface. Any invocation
     * of this method after initialization is done must throw {@link IllegalStateException} exception.
     *
     * @param supplier Message factory.
     * @param serializer Message serializer that also provides the direct type for registration.
     * @throws IgniteException In case of attempt to register message with direct type which is already registered.
     * @throws IllegalStateException On any invocation of this method when class which implements this interface
     * is already constructed.
     */
    default void register(Supplier<Message> supplier, MessageSerializer serializer) throws IgniteException {
        register(supplier.get().directType(), supplier, serializer);
    }

    /**
     * Creates new message instance of provided type.
     *
     * @param type Message type.
     * @return Message instance.
     */
    public Message create(short type);

    /**
     * Returns {@code MessageSerializer} for provided type.
     *
     * @param type Message type.
     * @return Message instance.
     */
    public MessageSerializer serializer(short type);
}
