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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;

/** */
public class CommunicationMessageSerializer {
    /** */
    private static final String PKG_NAME = "org.apache.ignite.internal.codegen";

    /** */
    private static final Map<Class<?>, MessageSerializer> MESSAGE_SERIALIZER = new ConcurrentHashMap<>();

    /** Delegate serialization to {@code Message} methods. */
    private static final MessageSerializer DEFAULT_SERIALIZER = new DefaultMessageSerializer();

    /**
     * @param msg Message to serialize.
     * @param buf Buffer to write the serialized message.
     * @param writer Writes the serialized message to the buffer field by field.
     * @return {@code true} if the message is fully write.
     */
    public static boolean writeTo(Message msg, ByteBuffer buf, MessageWriter writer) {
        MessageSerializer serializer = MESSAGE_SERIALIZER.get(msg.getClass());

        if (serializer == null)
            MESSAGE_SERIALIZER.put(msg.getClass(), serializer = serializer(msg));

        return serializer.writeTo(msg, buf, writer);
    }

    /**
     * @param msg Message to fill.
     * @param buf Buffer to read the serialized message.
     * @param reader Fills the message from the buffer field by field.
     * @return {@code true} if the message is fully read.
     */
    public static boolean readFrom(Message msg, ByteBuffer buf, MessageReader reader) {
        MessageSerializer serializer = MESSAGE_SERIALIZER.get(msg.getClass());

        if (serializer == null)
            MESSAGE_SERIALIZER.put(msg.getClass(), serializer = serializer(msg));

        return serializer.readFrom(msg, buf, reader);
    }

    /**
     * @param msg Message to find serializer for.
     * @return Serializer for the corresponding message.
     */
    private static MessageSerializer serializer(Message msg) {
        String serMsgClsName = PKG_NAME + msg.getClass().getName() + "Serializer";

        try {
            return (MessageSerializer)Class.forName(serMsgClsName).getDeclaredConstructor().newInstance();
        }
        catch (ClassNotFoundException e) {
            return DEFAULT_SERIALIZER;
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** Delegates serialization logic to message itself. */
    private static class DefaultMessageSerializer implements MessageSerializer {
        /** {@inheritDoc} */
        @Override public boolean writeTo(Message msg, ByteBuffer buf, MessageWriter writer) {
            return msg.writeTo(buf, writer);
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(Message msg, ByteBuffer buf, MessageReader reader) {
            return msg.readFrom(buf, reader);
        }
    }
}
