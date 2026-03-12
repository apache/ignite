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
public class TestCollectionsMessageSerializer implements MessageSerializer<TestCollectionsMessage> {
    /** */
    private final static MessageCollectionType affTopVersionListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), false);
    /** */
    private final static MessageCollectionType bitSetListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BIT_SET), false);
    /** */
    private final static MessageCollectionType bitSetSetCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BIT_SET), true);
    /** */
    private final static MessageCollectionType booleanArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), false);
    /** */
    private final static MessageCollectionType boxedBooleanListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BOOLEAN), false);
    /** */
    private final static MessageCollectionType boxedByteListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BYTE), false);
    /** */
    private final static MessageCollectionType boxedCharListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.CHAR), false);
    /** */
    private final static MessageCollectionType boxedDoubleListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE), false);
    /** */
    private final static MessageCollectionType boxedFloatListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.FLOAT), false);
    /** */
    private final static MessageCollectionType boxedIntListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT), false);
    /** */
    private final static MessageCollectionType boxedIntegerSetCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT), true);
    /** */
    private final static MessageCollectionType boxedLongListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.LONG), false);
    /** */
    private final static MessageCollectionType boxedShortListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.SHORT), false);
    /** */
    private final static MessageCollectionType byteArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BYTE_ARR), false);
    /** */
    private final static MessageCollectionType charArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.CHAR_ARR), false);
    /** */
    private final static MessageCollectionType doubleArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), false);
    /** */
    private final static MessageCollectionType floatArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.FLOAT_ARR), false);
    /** */
    private final static MessageCollectionType gridLongListListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), false);
    /** */
    private final static MessageCollectionType igniteUuidListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.IGNITE_UUID), false);
    /** */
    private final static MessageCollectionType intArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT_ARR), false);
    /** */
    private final static MessageCollectionType longArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.LONG_ARR), false);
    /** */
    private final static MessageCollectionType messageListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.MSG), false);
    /** */
    private final static MessageCollectionType shortArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.SHORT_ARR), false);
    /** */
    private final static MessageCollectionType stringListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.STRING), false);
    /** */
    private final static MessageCollectionType uuidListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.UUID), false);

    /** */
    @Override public boolean writeTo(TestCollectionsMessage msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection(msg.booleanArrayList, booleanArrayListCollDesc))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection(msg.byteArrayList, byteArrayListCollDesc))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection(msg.shortArrayList, shortArrayListCollDesc))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeCollection(msg.intArrayList, intArrayListCollDesc))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeCollection(msg.longArrayList, longArrayListCollDesc))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection(msg.charArrayList, charArrayListCollDesc))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection(msg.floatArrayList, floatArrayListCollDesc))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection(msg.doubleArrayList, doubleArrayListCollDesc))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeCollection(msg.stringList, stringListCollDesc))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection(msg.uuidList, uuidListCollDesc))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeCollection(msg.bitSetList, bitSetListCollDesc))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeCollection(msg.igniteUuidList, igniteUuidListCollDesc))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeCollection(msg.affTopVersionList, affTopVersionListCollDesc))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeCollection(msg.boxedBooleanList, boxedBooleanListCollDesc))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection(msg.boxedByteList, boxedByteListCollDesc))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollection(msg.boxedShortList, boxedShortListCollDesc))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeCollection(msg.boxedIntList, boxedIntListCollDesc))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeCollection(msg.boxedLongList, boxedLongListCollDesc))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeCollection(msg.boxedCharList, boxedCharListCollDesc))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeCollection(msg.boxedFloatList, boxedFloatListCollDesc))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeCollection(msg.boxedDoubleList, boxedDoubleListCollDesc))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeCollection(msg.messageList, messageListCollDesc))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeCollection(msg.gridLongListList, gridLongListListCollDesc))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeCollection(msg.boxedIntegerSet, boxedIntegerSetCollDesc))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeCollection(msg.bitSetSet, bitSetSetCollDesc))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(TestCollectionsMessage msg, MessageReader reader) {
        switch (reader.state()) {
            case 0:
                msg.booleanArrayList = reader.readCollection(booleanArrayListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.byteArrayList = reader.readCollection(byteArrayListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.shortArrayList = reader.readCollection(shortArrayListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                msg.intArrayList = reader.readCollection(intArrayListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                msg.longArrayList = reader.readCollection(longArrayListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                msg.charArrayList = reader.readCollection(charArrayListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                msg.floatArrayList = reader.readCollection(floatArrayListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                msg.doubleArrayList = reader.readCollection(doubleArrayListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                msg.stringList = reader.readCollection(stringListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                msg.uuidList = reader.readCollection(uuidListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                msg.bitSetList = reader.readCollection(bitSetListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                msg.igniteUuidList = reader.readCollection(igniteUuidListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                msg.affTopVersionList = reader.readCollection(affTopVersionListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                msg.boxedBooleanList = reader.readCollection(boxedBooleanListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                msg.boxedByteList = reader.readCollection(boxedByteListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                msg.boxedShortList = reader.readCollection(boxedShortListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                msg.boxedIntList = reader.readCollection(boxedIntListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                msg.boxedLongList = reader.readCollection(boxedLongListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                msg.boxedCharList = reader.readCollection(boxedCharListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                msg.boxedFloatList = reader.readCollection(boxedFloatListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                msg.boxedDoubleList = reader.readCollection(boxedDoubleListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                msg.messageList = reader.readCollection(messageListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                msg.gridLongListList = reader.readCollection(gridLongListListCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                msg.boxedIntegerSet = reader.readCollection(boxedIntegerSetCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                msg.bitSetSet = reader.readCollection(bitSetSetCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }
}