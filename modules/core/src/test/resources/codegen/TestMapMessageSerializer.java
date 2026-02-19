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
                if (!writer.writeMap(((TestMapMessage)msg).booleanArrayBoxedLongMap, MessageCollectionItemType.BOOLEAN_ARR, MessageCollectionItemType.LONG))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap(((TestMapMessage)msg).byteArrayBooleanArrayMap, MessageCollectionItemType.BYTE_ARR, MessageCollectionItemType.BOOLEAN_ARR))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMap(((TestMapMessage)msg).shortArrayByteArrayMap, MessageCollectionItemType.SHORT_ARR, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMap(((TestMapMessage)msg).intArrayShortArrayMap, MessageCollectionItemType.INT_ARR, MessageCollectionItemType.SHORT_ARR))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap(((TestMapMessage)msg).longArrayIntArrayMap, MessageCollectionItemType.LONG_ARR, MessageCollectionItemType.INT_ARR))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMap(((TestMapMessage)msg).charArrayLongArrayMap, MessageCollectionItemType.CHAR_ARR, MessageCollectionItemType.LONG_ARR))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMap(((TestMapMessage)msg).floatArrayCharArrayMap, MessageCollectionItemType.FLOAT_ARR, MessageCollectionItemType.CHAR_ARR))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMap(((TestMapMessage)msg).doubleArrayFloatArrayMap, MessageCollectionItemType.DOUBLE_ARR, MessageCollectionItemType.FLOAT_ARR))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMap(((TestMapMessage)msg).stringDoubleArrayMap, MessageCollectionItemType.STRING, MessageCollectionItemType.DOUBLE_ARR))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMap(((TestMapMessage)msg).uuidStringMap, MessageCollectionItemType.UUID, MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMap(((TestMapMessage)msg).bitSetUuidMap, MessageCollectionItemType.BIT_SET, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMap(((TestMapMessage)msg).igniteUuidBitSetMap, MessageCollectionItemType.IGNITE_UUID, MessageCollectionItemType.BIT_SET))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMap(((TestMapMessage)msg).affTopVersionIgniteUuidMap, MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION, MessageCollectionItemType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMap(((TestMapMessage)msg).boxedBooleanAffTopVersionMap, MessageCollectionItemType.BOOLEAN, MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMap(((TestMapMessage)msg).boxedByteBoxedBooleanMap, MessageCollectionItemType.BYTE, MessageCollectionItemType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMap(((TestMapMessage)msg).boxedShortBoxedByteMap, MessageCollectionItemType.SHORT, MessageCollectionItemType.BYTE))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeMap(((TestMapMessage)msg).boxedIntBoxedShortMap, MessageCollectionItemType.INT, MessageCollectionItemType.SHORT))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeMap(((TestMapMessage)msg).boxedLongBoxedIntMap, MessageCollectionItemType.LONG, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeMap(((TestMapMessage)msg).boxedCharBoxedLongMap, MessageCollectionItemType.CHAR, MessageCollectionItemType.LONG))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeMap(((TestMapMessage)msg).boxedFloatBoxedCharMap, MessageCollectionItemType.FLOAT, MessageCollectionItemType.CHAR))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeMap(((TestMapMessage)msg).boxedDoubleBoxedFloatMap, MessageCollectionItemType.DOUBLE, MessageCollectionItemType.FLOAT))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeMap(((TestMapMessage)msg).messageBoxedDoubleMap, MessageCollectionItemType.MSG, MessageCollectionItemType.DOUBLE))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeMap(((TestMapMessage)msg).integerGridLongListMap, MessageCollectionItemType.INT, MessageCollectionItemType.GRID_LONG_LIST))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeMap(((TestMapMessage)msg).gridLongListIntegerMap, MessageCollectionItemType.GRID_LONG_LIST, MessageCollectionItemType.INT))
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
                ((TestMapMessage)msg).booleanArrayBoxedLongMap = reader.readMap(MessageCollectionItemType.BOOLEAN_ARR, MessageCollectionItemType.LONG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                ((TestMapMessage)msg).byteArrayBooleanArrayMap = reader.readMap(MessageCollectionItemType.BYTE_ARR, MessageCollectionItemType.BOOLEAN_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                ((TestMapMessage)msg).shortArrayByteArrayMap = reader.readMap(MessageCollectionItemType.SHORT_ARR, MessageCollectionItemType.BYTE_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                ((TestMapMessage)msg).intArrayShortArrayMap = reader.readMap(MessageCollectionItemType.INT_ARR, MessageCollectionItemType.SHORT_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                ((TestMapMessage)msg).longArrayIntArrayMap = reader.readMap(MessageCollectionItemType.LONG_ARR, MessageCollectionItemType.INT_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                ((TestMapMessage)msg).charArrayLongArrayMap = reader.readMap(MessageCollectionItemType.CHAR_ARR, MessageCollectionItemType.LONG_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                ((TestMapMessage)msg).floatArrayCharArrayMap = reader.readMap(MessageCollectionItemType.FLOAT_ARR, MessageCollectionItemType.CHAR_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                ((TestMapMessage)msg).doubleArrayFloatArrayMap = reader.readMap(MessageCollectionItemType.DOUBLE_ARR, MessageCollectionItemType.FLOAT_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                ((TestMapMessage)msg).stringDoubleArrayMap = reader.readMap(MessageCollectionItemType.STRING, MessageCollectionItemType.DOUBLE_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                ((TestMapMessage)msg).uuidStringMap = reader.readMap(MessageCollectionItemType.UUID, MessageCollectionItemType.STRING, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                ((TestMapMessage)msg).bitSetUuidMap = reader.readMap(MessageCollectionItemType.BIT_SET, MessageCollectionItemType.UUID, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                ((TestMapMessage)msg).igniteUuidBitSetMap = reader.readMap(MessageCollectionItemType.IGNITE_UUID, MessageCollectionItemType.BIT_SET, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                ((TestMapMessage)msg).affTopVersionIgniteUuidMap = reader.readMap(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION, MessageCollectionItemType.IGNITE_UUID, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                ((TestMapMessage)msg).boxedBooleanAffTopVersionMap = reader.readMap(MessageCollectionItemType.BOOLEAN, MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                ((TestMapMessage)msg).boxedByteBoxedBooleanMap = reader.readMap(MessageCollectionItemType.BYTE, MessageCollectionItemType.BOOLEAN, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                ((TestMapMessage)msg).boxedShortBoxedByteMap = reader.readMap(MessageCollectionItemType.SHORT, MessageCollectionItemType.BYTE, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                ((TestMapMessage)msg).boxedIntBoxedShortMap = reader.readMap(MessageCollectionItemType.INT, MessageCollectionItemType.SHORT, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                ((TestMapMessage)msg).boxedLongBoxedIntMap = reader.readMap(MessageCollectionItemType.LONG, MessageCollectionItemType.INT, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                ((TestMapMessage)msg).boxedCharBoxedLongMap = reader.readMap(MessageCollectionItemType.CHAR, MessageCollectionItemType.LONG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                ((TestMapMessage)msg).boxedFloatBoxedCharMap = reader.readMap(MessageCollectionItemType.FLOAT, MessageCollectionItemType.CHAR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                ((TestMapMessage)msg).boxedDoubleBoxedFloatMap = reader.readMap(MessageCollectionItemType.DOUBLE, MessageCollectionItemType.FLOAT, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                ((TestMapMessage)msg).messageBoxedDoubleMap = reader.readMap(MessageCollectionItemType.MSG, MessageCollectionItemType.DOUBLE, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                ((TestMapMessage)msg).integerGridLongListMap = reader.readMap(MessageCollectionItemType.INT, MessageCollectionItemType.GRID_LONG_LIST, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                ((TestMapMessage)msg).gridLongListIntegerMap = reader.readMap(MessageCollectionItemType.GRID_LONG_LIST, MessageCollectionItemType.INT, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }
}
