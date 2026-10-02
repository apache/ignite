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

package org.apache.ignite.internal;

import java.util.List;
import org.apache.ignite.internal.GridTopicMessage;
import org.apache.ignite.internal.MessageSerializationContext;
import org.apache.ignite.internal.TestMarshalledArrayMapMessage;
import org.apache.ignite.plugin.extensions.communication.CollectionImplementationType;
import org.apache.ignite.plugin.extensions.communication.MessageArrayType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionType;
import org.apache.ignite.plugin.extensions.communication.MessageItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public final class TestMarshalledArrayMapMessageSerializer implements MessageSerializer<TestMarshalledArrayMapMessage> {
    /** */
    private static final MessageArrayType fixedMapKeysCollDesc = new MessageArrayType(new MessageItemType(MessageCollectionItemType.MSG), GridTopicMessage.class);
    /** */
    private static final MessageArrayType fixedMapValsCollDesc = new MessageArrayType(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.MSG), CollectionImplementationType.ARRAY_LIST), List.class);
    /** */
    private static final MessageArrayType mapKeysCollDesc = new MessageArrayType(new MessageItemType(MessageCollectionItemType.MSG), GridTopicMessage.class);
    /** */
    private static final MessageArrayType mapValsCollDesc = new MessageArrayType(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.MSG), CollectionImplementationType.ARRAY_LIST), List.class);

    /** */
    @Override public final boolean writeTo(TestMarshalledArrayMapMessage msg, MessageWriter writer, MessageSerializationContext ctx) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeObjectArray(msg.mapKeys, mapKeysCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeObjectArray(msg.mapVals, mapValsCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeObjectArray(msg.fixedMapKeys, fixedMapKeysCollDesc, ctx))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeObjectArray(msg.fixedMapVals, fixedMapValsCollDesc, ctx))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public final boolean readFrom(TestMarshalledArrayMapMessage msg, MessageReader reader, MessageSerializationContext ctx) {
        switch (reader.state()) {
            case 0:
                msg.mapKeys = reader.readObjectArray(mapKeysCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.mapVals = reader.readObjectArray(mapValsCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.fixedMapKeys = reader.readObjectArray(fixedMapKeysCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                msg.fixedMapVals = reader.readObjectArray(fixedMapValsCollDesc, ctx);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public final TestMarshalledArrayMapMessage createMessage() {
        return new TestMarshalledArrayMapMessage();
    }
}