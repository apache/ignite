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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Resolve-and-dispatch entry points for {@link MessageSerializer}: each looks up the serializer registered for the
 * message's direct type in the factory and delegates to it. The counterpart of {@code MessageMarshalling} on the
 * serialization side.
 */
public final class MessageSerialization {
    /** */
    private MessageSerialization() {
        // No-op.
    }

    /**
     * Writes the message using the serializer resolved from the factory.
     *
     * @param factory Message factory.
     * @param msg Message instance.
     * @param writer Writer.
     * @param <M> Message type.
     * @return Whether message was fully written.
     */
    public static <M extends Message> boolean writeTo(MessageFactory factory, M msg, MessageWriter writer) {
        return resolve(factory, msg).writeTo(msg, writer);
    }

    /**
     * Reads the message using the serializer resolved from the factory.
     *
     * @param factory Message factory.
     * @param msg Message instance.
     * @param reader Reader.
     * @param <M> Message type.
     * @return Whether message was fully read.
     */
    public static <M extends Message> boolean readFrom(MessageFactory factory, M msg, MessageReader reader) {
        return resolve(factory, msg).readFrom(msg, reader);
    }

    /** @return the serializer registered for {@code msg}'s direct type. */
    @SuppressWarnings("unchecked")
    private static <M extends Message> MessageSerializer<M> resolve(MessageFactory factory, M msg) {
        return (MessageSerializer<M>)factory.serializer(msg.directType());
    }
}
