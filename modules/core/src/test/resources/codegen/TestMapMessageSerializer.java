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

import org.apache.ignite.internal.TestMapMessage;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionType;
import org.apache.ignite.plugin.extensions.communication.MessageItemType;
import org.apache.ignite.plugin.extensions.communication.MessageMapType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestMapMessageSerializer implements MessageSerializer<TestMapMessage> {
    /** */
    private final static MessageMapType affTopVersionIgniteUuidMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), new MessageItemType(MessageCollectionItemType.IGNITE_UUID), false);
    /** */
    private final static MessageMapType bitSetUuidMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.BIT_SET), new MessageItemType(MessageCollectionItemType.UUID), false);
    /** */
    private final static MessageMapType booleanArrayBoxedLongMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), new MessageItemType(MessageCollectionItemType.LONG), false);
    /** */
    private final static MessageMapType boxedBooleanAffTopVersionMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.BOOLEAN), new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), false);
    /** */
    private final static MessageMapType boxedByteBoxedBooleanMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.BYTE), new MessageItemType(MessageCollectionItemType.BOOLEAN), false);
    /** */
    private final static MessageMapType boxedCharBoxedLongMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.CHAR), new MessageItemType(MessageCollectionItemType.LONG), false);
    /** */
    private final static MessageMapType boxedDoubleBoxedFloatMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.DOUBLE), new MessageItemType(MessageCollectionItemType.FLOAT), false);
    /** */
    private final static MessageMapType boxedFloatBoxedCharMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.FLOAT), new MessageItemType(MessageCollectionItemType.CHAR), false);
    /** */
    private final static MessageMapType boxedIntBoxedShortMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.INT), new MessageItemType(MessageCollectionItemType.SHORT), false);
    /** */
    private final static MessageMapType boxedLongBoxedIntMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.LONG), new MessageItemType(MessageCollectionItemType.INT), false);
    /** */
    private final static MessageMapType boxedShortBoxedByteMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.SHORT), new MessageItemType(MessageCollectionItemType.BYTE), false);
    /** */
    private final static MessageMapType byteArrayBooleanArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.BYTE_ARR), new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), false);
    /** */
    private final static MessageMapType charArrayLongArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.CHAR_ARR), new MessageItemType(MessageCollectionItemType.LONG_ARR), false);
    /** */
    private final static MessageMapType doubleArrayFloatArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), new MessageItemType(MessageCollectionItemType.FLOAT_ARR), false);
    /** */
    private final static MessageMapType floatArrayCharArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.FLOAT_ARR), new MessageItemType(MessageCollectionItemType.CHAR_ARR), false);
    /** */
    private final static MessageMapType gridLongListIntegerMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), new MessageItemType(MessageCollectionItemType.INT), false);
    /** */
    private final static MessageMapType gridlistDoubleMapUuidMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), new MessageMapType(new MessageItemType(MessageCollectionItemType.UUID), new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE), false), false), false);
    /** */
    private final static MessageMapType igniteUuidBitSetMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.IGNITE_UUID), new MessageItemType(MessageCollectionItemType.BIT_SET), false);
    /** */
    private final static MessageMapType intArrayShortArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.INT_ARR), new MessageItemType(MessageCollectionItemType.SHORT_ARR), false);
    /** */
    private final static MessageMapType integerGridLongListMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.INT), new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), false);
    /** */
    private final static MessageMapType longArrayIntArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.LONG_ARR), new MessageItemType(MessageCollectionItemType.INT_ARR), false);
    /** */
    private final static MessageMapType messageBoxedDoubleMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.MSG), new MessageItemType(MessageCollectionItemType.DOUBLE), false);
    /** */
    private final static MessageMapType shortArrayByteArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.SHORT_ARR), new MessageItemType(MessageCollectionItemType.BYTE_ARR), false);
    /** */
    private final static MessageMapType stringDoubleArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.STRING), new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), false);
    /** */
    private final static MessageMapType uuidStringMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.UUID), new MessageItemType(MessageCollectionItemType.STRING), false);

    /** */
    @Override public boolean writeTo(TestMapMessage msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMap(msg.booleanArrayBoxedLongMap, booleanArrayBoxedLongMapCollDesc))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap(msg.byteArrayBooleanArrayMap, byteArrayBooleanArrayMapCollDesc))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMap(msg.shortArrayByteArrayMap, shortArrayByteArrayMapCollDesc))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMap(msg.intArrayShortArrayMap, intArrayShortArrayMapCollDesc))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap(msg.longArrayIntArrayMap, longArrayIntArrayMapCollDesc))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMap(msg.charArrayLongArrayMap, charArrayLongArrayMapCollDesc))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMap(msg.floatArrayCharArrayMap, floatArrayCharArrayMapCollDesc))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMap(msg.doubleArrayFloatArrayMap, doubleArrayFloatArrayMapCollDesc))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMap(msg.stringDoubleArrayMap, stringDoubleArrayMapCollDesc))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMap(msg.uuidStringMap, uuidStringMapCollDesc))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMap(msg.bitSetUuidMap, bitSetUuidMapCollDesc))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMap(msg.igniteUuidBitSetMap, igniteUuidBitSetMapCollDesc))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMap(msg.affTopVersionIgniteUuidMap, affTopVersionIgniteUuidMapCollDesc))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMap(msg.boxedBooleanAffTopVersionMap, boxedBooleanAffTopVersionMapCollDesc))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMap(msg.boxedByteBoxedBooleanMap, boxedByteBoxedBooleanMapCollDesc))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMap(msg.boxedShortBoxedByteMap, boxedShortBoxedByteMapCollDesc))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeMap(msg.boxedIntBoxedShortMap, boxedIntBoxedShortMapCollDesc))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeMap(msg.boxedLongBoxedIntMap, boxedLongBoxedIntMapCollDesc))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeMap(msg.boxedCharBoxedLongMap, boxedCharBoxedLongMapCollDesc))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeMap(msg.boxedFloatBoxedCharMap, boxedFloatBoxedCharMapCollDesc))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeMap(msg.boxedDoubleBoxedFloatMap, boxedDoubleBoxedFloatMapCollDesc))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeMap(msg.messageBoxedDoubleMap, messageBoxedDoubleMapCollDesc))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeMap(msg.integerGridLongListMap, integerGridLongListMapCollDesc))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeMap(msg.gridLongListIntegerMap, gridLongListIntegerMapCollDesc))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeMap(msg.gridlistDoubleMapUuidMap, gridlistDoubleMapUuidMapCollDesc))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(TestMapMessage msg, MessageReader reader) {
        switch (reader.state()) {
            case 0:
                msg.booleanArrayBoxedLongMap = reader.readMap(booleanArrayBoxedLongMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.byteArrayBooleanArrayMap = reader.readMap(byteArrayBooleanArrayMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.shortArrayByteArrayMap = reader.readMap(shortArrayByteArrayMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                msg.intArrayShortArrayMap = reader.readMap(intArrayShortArrayMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                msg.longArrayIntArrayMap = reader.readMap(longArrayIntArrayMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                msg.charArrayLongArrayMap = reader.readMap(charArrayLongArrayMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                msg.floatArrayCharArrayMap = reader.readMap(floatArrayCharArrayMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                msg.doubleArrayFloatArrayMap = reader.readMap(doubleArrayFloatArrayMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                msg.stringDoubleArrayMap = reader.readMap(stringDoubleArrayMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                msg.uuidStringMap = reader.readMap(uuidStringMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                msg.bitSetUuidMap = reader.readMap(bitSetUuidMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                msg.igniteUuidBitSetMap = reader.readMap(igniteUuidBitSetMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                msg.affTopVersionIgniteUuidMap = reader.readMap(affTopVersionIgniteUuidMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                msg.boxedBooleanAffTopVersionMap = reader.readMap(boxedBooleanAffTopVersionMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                msg.boxedByteBoxedBooleanMap = reader.readMap(boxedByteBoxedBooleanMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                msg.boxedShortBoxedByteMap = reader.readMap(boxedShortBoxedByteMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                msg.boxedIntBoxedShortMap = reader.readMap(boxedIntBoxedShortMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                msg.boxedLongBoxedIntMap = reader.readMap(boxedLongBoxedIntMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                msg.boxedCharBoxedLongMap = reader.readMap(boxedCharBoxedLongMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                msg.boxedFloatBoxedCharMap = reader.readMap(boxedFloatBoxedCharMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                msg.boxedDoubleBoxedFloatMap = reader.readMap(boxedDoubleBoxedFloatMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                msg.messageBoxedDoubleMap = reader.readMap(messageBoxedDoubleMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                msg.integerGridLongListMap = reader.readMap(integerGridLongListMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                msg.gridLongListIntegerMap = reader.readMap(gridLongListIntegerMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                msg.gridlistDoubleMapUuidMap = reader.readMap(gridlistDoubleMapUuidMapCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }
}