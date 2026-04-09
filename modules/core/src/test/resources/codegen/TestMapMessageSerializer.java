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
    private static final MessageMapType affTopVersionIgniteUuidMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), new MessageItemType(MessageCollectionItemType.IGNITE_UUID), false);
    /** */
    private static final MessageMapType bitSetUuidMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.BIT_SET), new MessageItemType(MessageCollectionItemType.UUID), false);
    /** */
    private static final MessageMapType booleanArrayBoxedLongMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), new MessageItemType(MessageCollectionItemType.LONG), false);
    /** */
    private static final MessageMapType boxedBooleanAffTopVersionMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.BOOLEAN), new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), false);
    /** */
    private static final MessageMapType boxedByteBoxedBooleanMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.BYTE), new MessageItemType(MessageCollectionItemType.BOOLEAN), false);
    /** */
    private static final MessageMapType boxedCharBoxedLongMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.CHAR), new MessageItemType(MessageCollectionItemType.LONG), false);
    /** */
    private static final MessageMapType boxedDoubleBoxedFloatMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.DOUBLE), new MessageItemType(MessageCollectionItemType.FLOAT), false);
    /** */
    private static final MessageMapType boxedFloatBoxedCharMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.FLOAT), new MessageItemType(MessageCollectionItemType.CHAR), false);
    /** */
    private static final MessageMapType boxedIntBoxedShortMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.INT), new MessageItemType(MessageCollectionItemType.SHORT), false);
    /** */
    private static final MessageMapType boxedLongBoxedIntMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.LONG), new MessageItemType(MessageCollectionItemType.INT), false);
    /** */
    private static final MessageMapType boxedShortBoxedByteMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.SHORT), new MessageItemType(MessageCollectionItemType.BYTE), false);
    /** */
    private static final MessageMapType byteArrayBooleanArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.BYTE_ARR), new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), false);
    /** */
    private static final MessageMapType charArrayLongArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.CHAR_ARR), new MessageItemType(MessageCollectionItemType.LONG_ARR), false);
    /** */
    private static final MessageMapType doubleArrayFloatArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), new MessageItemType(MessageCollectionItemType.FLOAT_ARR), false);
    /** */
    private static final MessageMapType floatArrayCharArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.FLOAT_ARR), new MessageItemType(MessageCollectionItemType.CHAR_ARR), false);
    /** */
    private static final MessageMapType gridLongListIntegerMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), new MessageItemType(MessageCollectionItemType.INT), false);
    /** */
    private static final MessageMapType gridlistDoubleMapUuidMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), new MessageMapType(new MessageItemType(MessageCollectionItemType.UUID), new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE), false), false), false);
    /** */
    private static final MessageMapType igniteUuidBitSetMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.IGNITE_UUID), new MessageItemType(MessageCollectionItemType.BIT_SET), false);
    /** */
    private static final MessageMapType intArrayShortArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.INT_ARR), new MessageItemType(MessageCollectionItemType.SHORT_ARR), false);
    /** */
    private static final MessageMapType integerGridLongListMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.INT), new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), false);
    /** */
    private static final MessageMapType longArrayIntArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.LONG_ARR), new MessageItemType(MessageCollectionItemType.INT_ARR), false);
    /** */
    private static final MessageMapType messageBoxedDoubleMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.MSG), new MessageItemType(MessageCollectionItemType.DOUBLE), false);
    /** */
    private static final MessageMapType shortArrayByteArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.SHORT_ARR), new MessageItemType(MessageCollectionItemType.BYTE_ARR), false);
    /** */
    private static final MessageMapType stringDoubleArrayMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.STRING), new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), false);
    /** */
    private static final MessageMapType uuidStringMapCollDesc = new MessageMapType(new MessageItemType(MessageCollectionItemType.UUID), new MessageItemType(MessageCollectionItemType.STRING), false);

    /** */
    @Override public boolean writeTo(TestMapMessage msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMap(msg.booleanArrayBoxedLongMap, booleanArrayBoxedLongMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap(msg.byteArrayBooleanArrayMap, byteArrayBooleanArrayMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMap(msg.shortArrayByteArrayMap, shortArrayByteArrayMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMap(msg.intArrayShortArrayMap, intArrayShortArrayMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap(msg.longArrayIntArrayMap, longArrayIntArrayMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMap(msg.charArrayLongArrayMap, charArrayLongArrayMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMap(msg.floatArrayCharArrayMap, floatArrayCharArrayMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMap(msg.doubleArrayFloatArrayMap, doubleArrayFloatArrayMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMap(msg.stringDoubleArrayMap, stringDoubleArrayMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMap(msg.uuidStringMap, uuidStringMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMap(msg.bitSetUuidMap, bitSetUuidMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMap(msg.igniteUuidBitSetMap, igniteUuidBitSetMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMap(msg.affTopVersionIgniteUuidMap, affTopVersionIgniteUuidMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMap(msg.boxedBooleanAffTopVersionMap, boxedBooleanAffTopVersionMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMap(msg.boxedByteBoxedBooleanMap, boxedByteBoxedBooleanMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMap(msg.boxedShortBoxedByteMap, boxedShortBoxedByteMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeMap(msg.boxedIntBoxedShortMap, boxedIntBoxedShortMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeMap(msg.boxedLongBoxedIntMap, boxedLongBoxedIntMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeMap(msg.boxedCharBoxedLongMap, boxedCharBoxedLongMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeMap(msg.boxedFloatBoxedCharMap, boxedFloatBoxedCharMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeMap(msg.boxedDoubleBoxedFloatMap, boxedDoubleBoxedFloatMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeMap(msg.messageBoxedDoubleMap, messageBoxedDoubleMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeMap(msg.integerGridLongListMap, integerGridLongListMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeMap(msg.gridLongListIntegerMap, gridLongListIntegerMapCollDesc, msg))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeMap(msg.gridlistDoubleMapUuidMap, gridlistDoubleMapUuidMapCollDesc, msg))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(TestMapMessage msg, MessageReader reader) {
        switch (reader.state()) {
            case 0:
                msg.booleanArrayBoxedLongMap = reader.readMap(booleanArrayBoxedLongMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.byteArrayBooleanArrayMap = reader.readMap(byteArrayBooleanArrayMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.shortArrayByteArrayMap = reader.readMap(shortArrayByteArrayMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                msg.intArrayShortArrayMap = reader.readMap(intArrayShortArrayMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                msg.longArrayIntArrayMap = reader.readMap(longArrayIntArrayMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                msg.charArrayLongArrayMap = reader.readMap(charArrayLongArrayMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                msg.floatArrayCharArrayMap = reader.readMap(floatArrayCharArrayMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                msg.doubleArrayFloatArrayMap = reader.readMap(doubleArrayFloatArrayMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                msg.stringDoubleArrayMap = reader.readMap(stringDoubleArrayMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                msg.uuidStringMap = reader.readMap(uuidStringMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                msg.bitSetUuidMap = reader.readMap(bitSetUuidMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                msg.igniteUuidBitSetMap = reader.readMap(igniteUuidBitSetMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                msg.affTopVersionIgniteUuidMap = reader.readMap(affTopVersionIgniteUuidMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                msg.boxedBooleanAffTopVersionMap = reader.readMap(boxedBooleanAffTopVersionMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                msg.boxedByteBoxedBooleanMap = reader.readMap(boxedByteBoxedBooleanMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                msg.boxedShortBoxedByteMap = reader.readMap(boxedShortBoxedByteMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                msg.boxedIntBoxedShortMap = reader.readMap(boxedIntBoxedShortMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                msg.boxedLongBoxedIntMap = reader.readMap(boxedLongBoxedIntMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                msg.boxedCharBoxedLongMap = reader.readMap(boxedCharBoxedLongMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                msg.boxedFloatBoxedCharMap = reader.readMap(boxedFloatBoxedCharMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                msg.boxedDoubleBoxedFloatMap = reader.readMap(boxedDoubleBoxedFloatMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                msg.messageBoxedDoubleMap = reader.readMap(messageBoxedDoubleMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                msg.integerGridLongListMap = reader.readMap(integerGridLongListMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                msg.gridLongListIntegerMap = reader.readMap(gridLongListIntegerMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                msg.gridlistDoubleMapUuidMap = reader.readMap(gridlistDoubleMapUuidMapCollDesc, msg);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }
}