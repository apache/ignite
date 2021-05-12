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

package org.apache.ignite.network.scalecube.message;

import java.util.Map;
import org.apache.ignite.network.internal.MessageReader;
import org.apache.ignite.network.message.MessageDeserializer;
import org.apache.ignite.network.message.MessageMappingException;
import org.apache.ignite.network.message.MessageSerializationFactory;
import org.apache.ignite.network.message.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

/**
 * Serialization factory for {@link ScaleCubeMessage}.
 * TODO: IGNITE-14649 This class should be generated.
 */
public class ScaleCubeMessageSerializationFactory implements MessageSerializationFactory<ScaleCubeMessage> {
    /** {@inheritDoc} */
    @Override public MessageDeserializer<ScaleCubeMessage> createDeserializer() {
        return new MessageDeserializer<>() {
            /** */
            ScaleCubeMessage obj;

            /** */
            byte[] array;

            /** */
            Map<String, String> headers;

            /** {@inheritDoc} */
            @Override public boolean readMessage(MessageReader reader) throws MessageMappingException {
                if (!reader.beforeMessageRead())
                    return false;

                switch (reader.state()) {
                    case 0:
                        array = reader.readByteArray("array");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                    //noinspection fallthrough
                    case 1:
                        headers = reader.readMap("headers", MessageCollectionItemType.STRING, MessageCollectionItemType.STRING, false);

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                }

                obj = new ScaleCubeMessage(array, headers);

                return reader.afterMessageRead(ScaleCubeMessage.class);
            }

            /** {@inheritDoc} */
            @Override public Class<ScaleCubeMessage> klass() {
                return ScaleCubeMessage.class;
            }

            /** {@inheritDoc} */
            @Override public ScaleCubeMessage getMessage() {
                return obj;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public MessageSerializer<ScaleCubeMessage> createSerializer() {
        return (message, writer) -> {
            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(message.directType(), (byte) 2))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 0:
                    if (!writer.writeByteArray("array", message.getArray()))
                        return false;

                    writer.incrementState();

                //noinspection fallthrough
                case 1:
                    if (!writer.writeMap("headers", message.getHeaders(), MessageCollectionItemType.STRING, MessageCollectionItemType.STRING))
                        return false;

                    writer.incrementState();

            }

            return true;
        };
    }
}
