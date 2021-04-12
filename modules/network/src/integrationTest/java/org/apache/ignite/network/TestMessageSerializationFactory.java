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

package org.apache.ignite.network;

import java.util.Map;
import org.apache.ignite.network.internal.MessageReader;
import org.apache.ignite.network.message.MessageDeserializer;
import org.apache.ignite.network.message.MessageMappingException;
import org.apache.ignite.network.message.MessageSerializer;
import org.apache.ignite.network.message.MessageSerializationFactory;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

/**
 * Mapper for {@link TestMessage}.
 */
public class TestMessageSerializationFactory implements MessageSerializationFactory<TestMessage> {
    /** {@inheritDoc} */
    @Override public MessageDeserializer<TestMessage> createDeserializer() {
        return new MessageDeserializer<>() {

            private TestMessage obj;

            private String msg;
            private Map<Integer, String> map;

            @Override
            public boolean readMessage(MessageReader reader) throws MessageMappingException {
                if (!reader.beforeMessageRead())
                    return false;

                switch (reader.state()) {
                    case 0:
                        map = reader.readMap("map", MessageCollectionItemType.INT, MessageCollectionItemType.STRING, false);

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                        //noinspection fallthrough
                    case 1:
                        msg = reader.readString("msg");

                        if (!reader.isLastRead())
                            return false;

                        reader.incrementState();

                }

                obj = new TestMessage(msg, map);

                return reader.afterMessageRead(TestMessage.class);
            }

            @Override
            public Class<TestMessage> klass() {
                return TestMessage.class;
            }

            @Override
            public TestMessage getMessage() {
                return obj;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public MessageSerializer<TestMessage> createSerializer() {
        return (message, writer) -> {
            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(message.directType(), (byte) 1))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 0:
                    if (!writer.writeMap("map", message.getMap(), MessageCollectionItemType.INT, MessageCollectionItemType.STRING))
                        return false;

                    writer.incrementState();

                //noinspection fallthrough
                case 1:
                    if (!writer.writeString("msg", message.msg()))
                        return false;

                    writer.incrementState();

            }

            return true;
        };
    }
}
