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

package org.apache.ignite.network.scalecube;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.network.message.MessageDeserializer;
import org.apache.ignite.network.message.MessageMapperProvider;
import org.apache.ignite.network.message.MessageMappingException;
import org.apache.ignite.network.message.MessageSerializer;
import org.apache.ignite.network.message.NetworkMessage;

/**
 * Serializes and deserialized messages in ScaleCube cluster.
 */
class ScaleCubeMessageCodec implements MessageCodec {
    /** Header name for {@link NetworkMessage#directType()}. */
    public static final String HEADER_MESSAGE_TYPE = "type";

    /** Message mappers, messageMapperProviders[message type] -> message mapper provider for message with message type. */
    private final List<MessageMapperProvider<?>> messageMappers;

    /**
     * Constructor.
     * @param map Message mapper map.
     */
    ScaleCubeMessageCodec(List<MessageMapperProvider<?>> mappers) {
        messageMappers = mappers;
    }

    /** {@inheritDoc} */
    @Override public Message deserialize(InputStream stream) throws Exception {
        Message.Builder builder = Message.builder();
        try (ObjectInputStream ois = new ObjectInputStream(stream)) {
            // headers
            int headersSize = ois.readInt();
            Map<String, String> headers = new HashMap<>(headersSize);
            for (int i = 0; i < headersSize; i++) {
                String name = ois.readUTF();
                String value = (String) ois.readObject();
                headers.put(name, value);
            }

            builder.headers(headers);

            String typeString = headers.get(HEADER_MESSAGE_TYPE);

            if (typeString == null) {
                builder.data(ois.readObject());
                return builder.build();
            }

            short type;
            try {
                type = Short.parseShort(typeString);
            }
            catch (NumberFormatException e) {
                throw new MessageMappingException("Type is not short", e);
            }

            MessageMapperProvider mapperProvider = messageMappers.get(type);

            assert mapperProvider != null : "No mapper provider defined for type " + type;

            MessageDeserializer deserializer = mapperProvider.createDeserializer();

            NetworkMessage message = deserializer.readMessage(new ScaleCubeMessageReader(ois));

            builder.data(message);
        }
        return builder.build();
    }

    /** {@inheritDoc} */
    @Override public void serialize(Message message, OutputStream stream) throws Exception {
        final Object data = message.data();

        if (!(data instanceof NetworkMessage)) {
            try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
                message.writeExternal(oos);
            }
            return;
        }

        Map<String, String> headers = message.headers();

        assert headers.containsKey(HEADER_MESSAGE_TYPE) : "Missing message type header";

        try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
            // headers
            oos.writeInt(headers.size());
            for (Map.Entry<String, String> header : headers.entrySet()) {
                oos.writeUTF(header.getKey());
                oos.writeObject(header.getValue());
            }

            assert data instanceof NetworkMessage : "Message data is not an instance of NetworkMessage";

            NetworkMessage msg = (NetworkMessage) data;
            MessageMapperProvider mapper = messageMappers.get(msg.directType());

            assert mapper != null : "No mapper provider defined for type " + msg.getClass();

            MessageSerializer serializer = mapper.createSerializer();

            serializer.writeMessage(msg, new ScaleCubeMessageWriter(oos));

            oos.flush();
        }
    }
}
