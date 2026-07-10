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

import org.apache.ignite.internal.TestMarshalledMapMessage;
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
public class TestMarshalledMapMessageSerializer implements MessageSerializer<TestMarshalledMapMessage> {
    /** */
    private static final MessageCollectionType mapKeysCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.GRID_CACHE_VERSION), false);
    /** */
    private static final MessageCollectionType mapValsCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.GRID_CACHE_VERSION), false);

    /** */
    @Override public boolean writeTo(TestMarshalledMapMessage msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection(msg.mapKeys, mapKeysCollDesc))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection(msg.mapVals, mapValsCollDesc))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(TestMarshalledMapMessage msg, MessageReader reader) {
        switch (reader.state()) {
            case 0:
                msg.mapKeys = reader.readCollection(mapKeysCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.mapVals = reader.readCollection(mapValsCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }
}