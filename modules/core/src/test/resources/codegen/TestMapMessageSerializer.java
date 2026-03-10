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
import org.apache.ignite.plugin.extensions.communication.Message;
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
public class TestMapMessageSerializer implements MessageSerializer {
    /** */
    @Override public boolean writeTo(Message m, MessageWriter writer) {
        TestMapMessage msg = (TestMapMessage)m;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMap(((TestMapMessage)msg).booleanArrayBoxedLongMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), new MessageItemType(MessageCollectionItemType.LONG), false)))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap(((TestMapMessage)msg).byteArrayBooleanArrayMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.BYTE_ARR), new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), false)))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMap(((TestMapMessage)msg).shortArrayByteArrayMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.SHORT_ARR), new MessageItemType(MessageCollectionItemType.BYTE_ARR), false)))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMap(((TestMapMessage)msg).intArrayShortArrayMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.INT_ARR), new MessageItemType(MessageCollectionItemType.SHORT_ARR), false)))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap(((TestMapMessage)msg).longArrayIntArrayMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.LONG_ARR), new MessageItemType(MessageCollectionItemType.INT_ARR), false)))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMap(((TestMapMessage)msg).charArrayLongArrayMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.CHAR_ARR), new MessageItemType(MessageCollectionItemType.LONG_ARR), false)))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMap(((TestMapMessage)msg).floatArrayCharArrayMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.FLOAT_ARR), new MessageItemType(MessageCollectionItemType.CHAR_ARR), false)))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMap(((TestMapMessage)msg).doubleArrayFloatArrayMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), new MessageItemType(MessageCollectionItemType.FLOAT_ARR), false)))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMap(((TestMapMessage)msg).stringDoubleArrayMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.STRING), new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), false)))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMap(((TestMapMessage)msg).uuidStringMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.UUID), new MessageItemType(MessageCollectionItemType.STRING), false)))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMap(((TestMapMessage)msg).bitSetUuidMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.BIT_SET), new MessageItemType(MessageCollectionItemType.UUID), false)))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMap(((TestMapMessage)msg).igniteUuidBitSetMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.IGNITE_UUID), new MessageItemType(MessageCollectionItemType.BIT_SET), false)))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMap(((TestMapMessage)msg).affTopVersionIgniteUuidMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), new MessageItemType(MessageCollectionItemType.IGNITE_UUID), false)))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMap(((TestMapMessage)msg).boxedBooleanAffTopVersionMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.BOOLEAN), new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), false)))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMap(((TestMapMessage)msg).boxedByteBoxedBooleanMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.BYTE), new MessageItemType(MessageCollectionItemType.BOOLEAN), false)))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMap(((TestMapMessage)msg).boxedShortBoxedByteMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.SHORT), new MessageItemType(MessageCollectionItemType.BYTE), false)))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeMap(((TestMapMessage)msg).boxedIntBoxedShortMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.INT), new MessageItemType(MessageCollectionItemType.SHORT), false)))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeMap(((TestMapMessage)msg).boxedLongBoxedIntMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.LONG), new MessageItemType(MessageCollectionItemType.INT), false)))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeMap(((TestMapMessage)msg).boxedCharBoxedLongMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.CHAR), new MessageItemType(MessageCollectionItemType.LONG), false)))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeMap(((TestMapMessage)msg).boxedFloatBoxedCharMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.FLOAT), new MessageItemType(MessageCollectionItemType.CHAR), false)))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeMap(((TestMapMessage)msg).boxedDoubleBoxedFloatMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.DOUBLE), new MessageItemType(MessageCollectionItemType.FLOAT), false)))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeMap(((TestMapMessage)msg).messageBoxedDoubleMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.MSG), new MessageItemType(MessageCollectionItemType.DOUBLE), false)))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeMap(((TestMapMessage)msg).integerGridLongListMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.INT), new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), false)))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeMap(((TestMapMessage)msg).gridLongListIntegerMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), new MessageItemType(MessageCollectionItemType.INT), false)))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeMap(((TestMapMessage)msg).gridlistDoubleMapUuidMap, new MessageMapType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), new MessageMapType(new MessageItemType(MessageCollectionItemType.UUID), new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE), false), false), false)))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(Message m, MessageReader reader) {
        TestMapMessage msg = (TestMapMessage)m;

        switch (reader.state()) {
            case 0:
                ((TestMapMessage)msg).booleanArrayBoxedLongMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), new MessageItemType(MessageCollectionItemType.LONG), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                ((TestMapMessage)msg).byteArrayBooleanArrayMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.BYTE_ARR), new MessageItemType(MessageCollectionItemType.BOOLEAN_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                ((TestMapMessage)msg).shortArrayByteArrayMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.SHORT_ARR), new MessageItemType(MessageCollectionItemType.BYTE_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                ((TestMapMessage)msg).intArrayShortArrayMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.INT_ARR), new MessageItemType(MessageCollectionItemType.SHORT_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                ((TestMapMessage)msg).longArrayIntArrayMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.LONG_ARR), new MessageItemType(MessageCollectionItemType.INT_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                ((TestMapMessage)msg).charArrayLongArrayMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.CHAR_ARR), new MessageItemType(MessageCollectionItemType.LONG_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                ((TestMapMessage)msg).floatArrayCharArrayMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.FLOAT_ARR), new MessageItemType(MessageCollectionItemType.CHAR_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                ((TestMapMessage)msg).doubleArrayFloatArrayMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), new MessageItemType(MessageCollectionItemType.FLOAT_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                ((TestMapMessage)msg).stringDoubleArrayMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.STRING), new MessageItemType(MessageCollectionItemType.DOUBLE_ARR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                ((TestMapMessage)msg).uuidStringMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.UUID), new MessageItemType(MessageCollectionItemType.STRING), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                ((TestMapMessage)msg).bitSetUuidMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.BIT_SET), new MessageItemType(MessageCollectionItemType.UUID), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                ((TestMapMessage)msg).igniteUuidBitSetMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.IGNITE_UUID), new MessageItemType(MessageCollectionItemType.BIT_SET), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                ((TestMapMessage)msg).affTopVersionIgniteUuidMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), new MessageItemType(MessageCollectionItemType.IGNITE_UUID), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                ((TestMapMessage)msg).boxedBooleanAffTopVersionMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.BOOLEAN), new MessageItemType(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                ((TestMapMessage)msg).boxedByteBoxedBooleanMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.BYTE), new MessageItemType(MessageCollectionItemType.BOOLEAN), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                ((TestMapMessage)msg).boxedShortBoxedByteMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.SHORT), new MessageItemType(MessageCollectionItemType.BYTE), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                ((TestMapMessage)msg).boxedIntBoxedShortMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.INT), new MessageItemType(MessageCollectionItemType.SHORT), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                ((TestMapMessage)msg).boxedLongBoxedIntMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.LONG), new MessageItemType(MessageCollectionItemType.INT), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                ((TestMapMessage)msg).boxedCharBoxedLongMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.CHAR), new MessageItemType(MessageCollectionItemType.LONG), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                ((TestMapMessage)msg).boxedFloatBoxedCharMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.FLOAT), new MessageItemType(MessageCollectionItemType.CHAR), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                ((TestMapMessage)msg).boxedDoubleBoxedFloatMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.DOUBLE), new MessageItemType(MessageCollectionItemType.FLOAT), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                ((TestMapMessage)msg).messageBoxedDoubleMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.MSG), new MessageItemType(MessageCollectionItemType.DOUBLE), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                ((TestMapMessage)msg).integerGridLongListMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.INT), new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                ((TestMapMessage)msg).gridLongListIntegerMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), new MessageItemType(MessageCollectionItemType.INT), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                ((TestMapMessage)msg).gridlistDoubleMapUuidMap = reader.readMap(new MessageMapType(new MessageItemType(MessageCollectionItemType.GRID_LONG_LIST), new MessageMapType(new MessageItemType(MessageCollectionItemType.UUID), new MessageCollectionType(new MessageItemType(MessageCollectionItemType.DOUBLE), false), false), false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }
}