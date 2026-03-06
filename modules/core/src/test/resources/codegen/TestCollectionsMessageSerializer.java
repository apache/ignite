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

import org.apache.ignite.internal.TestCollectionsMessage;
import org.apache.ignite.plugin.extensions.communication.Message;
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
public class TestCollectionsMessageSerializer implements MessageSerializer {
    /** */
    @Override public boolean writeTo(Message m, MessageWriter writer) {
        TestCollectionsMessage msg = (TestCollectionsMessage)m;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).booleanArrayList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), false)))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).byteArrayList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BYTE_ARR), false)))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).shortArrayList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.SHORT_ARR), false)))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).intArrayList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT_ARR), false)))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).longArrayList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.LONG_ARR), false)))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).charArrayList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.CHAR_ARR), false)))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).floatArrayList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.FLOAT_ARR), false)))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).doubleArrayList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), false)))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).stringList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.STRING), false)))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).uuidList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.UUID), false)))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).bitSetList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BIT_SET), false)))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).igniteUuidList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.IGNITE_UUID), false)))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).affTopVersionList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), false)))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).boxedBooleanList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BOOLEAN), false)))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).boxedByteList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BYTE), false)))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).boxedShortList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.SHORT), false)))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).boxedIntList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT), false)))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).boxedLongList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.LONG), false)))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).boxedCharList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.CHAR), false)))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).boxedFloatList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.FLOAT), false)))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).boxedDoubleList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE), false)))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).messageList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.MSG), false)))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).gridLongListList, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), false)))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).boxedIntegerSet, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT), true)))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeCollection(((TestCollectionsMessage)msg).bitSetSet, new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BIT_SET), true)))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(Message m, MessageReader reader) {
        TestCollectionsMessage msg = (TestCollectionsMessage)m;

        switch (reader.state()) {
            case 0:
                ((TestCollectionsMessage)msg).booleanArrayList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                ((TestCollectionsMessage)msg).byteArrayList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BYTE_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                ((TestCollectionsMessage)msg).shortArrayList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.SHORT_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                ((TestCollectionsMessage)msg).intArrayList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                ((TestCollectionsMessage)msg).longArrayList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.LONG_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                ((TestCollectionsMessage)msg).charArrayList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.CHAR_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                ((TestCollectionsMessage)msg).floatArrayList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.FLOAT_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                ((TestCollectionsMessage)msg).doubleArrayList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                ((TestCollectionsMessage)msg).stringList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.STRING), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                ((TestCollectionsMessage)msg).uuidList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.UUID), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                ((TestCollectionsMessage)msg).bitSetList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BIT_SET), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                ((TestCollectionsMessage)msg).igniteUuidList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.IGNITE_UUID), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                ((TestCollectionsMessage)msg).affTopVersionList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                ((TestCollectionsMessage)msg).boxedBooleanList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BOOLEAN), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                ((TestCollectionsMessage)msg).boxedByteList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BYTE), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                ((TestCollectionsMessage)msg).boxedShortList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.SHORT), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                ((TestCollectionsMessage)msg).boxedIntList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                ((TestCollectionsMessage)msg).boxedLongList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.LONG), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                ((TestCollectionsMessage)msg).boxedCharList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.CHAR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                ((TestCollectionsMessage)msg).boxedFloatList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.FLOAT), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                ((TestCollectionsMessage)msg).boxedDoubleList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                ((TestCollectionsMessage)msg).messageList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.MSG), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                ((TestCollectionsMessage)msg).gridLongListList = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                ((TestCollectionsMessage)msg).boxedIntegerSet = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT), true));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                ((TestCollectionsMessage)msg).bitSetSet = reader.readCollection(new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BIT_SET), true));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }
}