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
    private static final MessageCollectionType affTopVersionListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), false);
    /** */
    private static final MessageCollectionType bitSetListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BIT_SET), false);
    /** */
    private static final MessageCollectionType bitSetSetCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BIT_SET), true);
    /** */
    private static final MessageCollectionType booleanArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), false);
    /** */
    private static final MessageCollectionType boxedBooleanListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BOOLEAN), false);
    /** */
    private static final MessageCollectionType boxedByteListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BYTE), false);
    /** */
    private static final MessageCollectionType boxedCharListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.CHAR), false);
    /** */
    private static final MessageCollectionType boxedDoubleListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE), false);
    /** */
    private static final MessageCollectionType boxedFloatListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.FLOAT), false);
    /** */
    private static final MessageCollectionType boxedIntListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT), false);
    /** */
    private static final MessageCollectionType boxedIntegerSetCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT), true);
    /** */
    private static final MessageCollectionType boxedLongListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.LONG), false);
    /** */
    private static final MessageCollectionType boxedShortListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.SHORT), false);
    /** */
    private static final MessageCollectionType byteArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.BYTE_ARR), false);
    /** */
    private static final MessageCollectionType charArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.CHAR_ARR), false);
    /** */
    private static final MessageCollectionType doubleArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), false);
    /** */
    private static final MessageCollectionType floatArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.FLOAT_ARR), false);
    /** */
    private static final MessageCollectionType gridLongListListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), false);
    /** */
    private static final MessageCollectionType igniteUuidListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.IGNITE_UUID), false);
    /** */
    private static final MessageCollectionType intArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT_ARR), false);
    /** */
    private static final MessageCollectionType longArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.LONG_ARR), false);
    /** */
    private static final MessageCollectionType messageListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.MSG), false);
    /** */
    private static final MessageCollectionType shortArrayListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.SHORT_ARR), false);
    /** */
    private static final MessageCollectionType stringListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.STRING), false);
    /** */
    private static final MessageCollectionType uuidListCollDesc = new MessageCollectionType(new MessageItemType(MessageCollectionItemType.UUID), false);

    /** */
    @Override public boolean writeTo(TestCollectionsMessage msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection(msg.booleanArrayList, booleanArrayListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection(msg.byteArrayList, byteArrayListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection(msg.shortArrayList, shortArrayListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeCollection(msg.intArrayList, intArrayListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeCollection(msg.longArrayList, longArrayListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection(msg.charArrayList, charArrayListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection(msg.floatArrayList, floatArrayListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection(msg.doubleArrayList, doubleArrayListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeCollection(msg.stringList, stringListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection(msg.uuidList, uuidListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeCollection(msg.bitSetList, bitSetListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeCollection(msg.igniteUuidList, igniteUuidListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeCollection(msg.affTopVersionList, affTopVersionListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeCollection(msg.boxedBooleanList, boxedBooleanListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection(msg.boxedByteList, boxedByteListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollection(msg.boxedShortList, boxedShortListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeCollection(msg.boxedIntList, boxedIntListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeCollection(msg.boxedLongList, boxedLongListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeCollection(msg.boxedCharList, boxedCharListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeCollection(msg.boxedFloatList, boxedFloatListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeCollection(msg.boxedDoubleList, boxedDoubleListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeCollection(msg.messageList, messageListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeCollection(msg.gridLongListList, gridLongListListCollDesc, msg))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeCollection(msg.boxedIntegerSet, boxedIntegerSetCollDesc, msg))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeCollection(msg.bitSetSet, bitSetSetCollDesc, msg))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(TestCollectionsMessage msg, MessageReader reader) {
        switch (reader.state()) {
            case 0:
                msg.booleanArrayList = reader.readCollection(booleanArrayListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.byteArrayList = reader.readCollection(byteArrayListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.shortArrayList = reader.readCollection(shortArrayListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                msg.intArrayList = reader.readCollection(intArrayListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                msg.longArrayList = reader.readCollection(longArrayListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                msg.charArrayList = reader.readCollection(charArrayListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                msg.floatArrayList = reader.readCollection(floatArrayListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                msg.doubleArrayList = reader.readCollection(doubleArrayListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                msg.stringList = reader.readCollection(stringListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                msg.uuidList = reader.readCollection(uuidListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                msg.bitSetList = reader.readCollection(bitSetListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                msg.igniteUuidList = reader.readCollection(igniteUuidListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                msg.affTopVersionList = reader.readCollection(affTopVersionListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                msg.boxedBooleanList = reader.readCollection(boxedBooleanListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                msg.boxedByteList = reader.readCollection(boxedByteListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                msg.boxedShortList = reader.readCollection(boxedShortListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                msg.boxedIntList = reader.readCollection(boxedIntListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                msg.boxedLongList = reader.readCollection(boxedLongListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                msg.boxedCharList = reader.readCollection(boxedCharListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                msg.boxedFloatList = reader.readCollection(boxedFloatListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                msg.boxedDoubleList = reader.readCollection(boxedDoubleListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                msg.messageList = reader.readCollection(messageListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                msg.gridLongListList = reader.readCollection(gridLongListListCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                msg.boxedIntegerSet = reader.readCollection(boxedIntegerSetCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                msg.bitSetSet = reader.readCollection(bitSetSetCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }
}