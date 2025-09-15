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

package org.apache.ignite.internal.codegen;

import java.nio.ByteBuffer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.internal.TestMapMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestMapMessageSerializer implements MessageSerializer {
    /** */
    @Override public boolean writeTo(Message m, ByteBuffer buf, MessageWriter writer) {
        TestMapMessage msg = (TestMapMessage)m;

        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMap(msg.booleanArrayBoxedLongMap(), MessageCollectionItemType.BOOLEAN_ARR, MessageCollectionItemType.LONG))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap(msg.byteArrayBooleanArrayMap(), MessageCollectionItemType.BYTE_ARR, MessageCollectionItemType.BOOLEAN_ARR))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMap(msg.shortArrayByteArrayMap(), MessageCollectionItemType.SHORT_ARR, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMap(msg.intArrayShortArrayMap(), MessageCollectionItemType.INT_ARR, MessageCollectionItemType.SHORT_ARR))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap(msg.longArrayIntArrayMap(), MessageCollectionItemType.LONG_ARR, MessageCollectionItemType.INT_ARR))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMap(msg.charArrayLongArrayMap(), MessageCollectionItemType.CHAR_ARR, MessageCollectionItemType.LONG_ARR))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMap(msg.floatArrayCharArrayMap(), MessageCollectionItemType.FLOAT_ARR, MessageCollectionItemType.CHAR_ARR))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMap(msg.doubleArrayFloatArrayMap(), MessageCollectionItemType.DOUBLE_ARR, MessageCollectionItemType.FLOAT_ARR))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMap(msg.stringDoubleArrayMap(), MessageCollectionItemType.STRING, MessageCollectionItemType.DOUBLE_ARR))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMap(msg.uuidStringMap(), MessageCollectionItemType.UUID, MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMap(msg.bitSetUuidMap(), MessageCollectionItemType.BIT_SET, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMap(msg.igniteUuidBitSetMap(), MessageCollectionItemType.IGNITE_UUID, MessageCollectionItemType.BIT_SET))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMap(msg.affTopVersionIgniteUuidMap(), MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION, MessageCollectionItemType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMap(msg.boxedBooleanAffTopVersionMap(), MessageCollectionItemType.BOOLEAN, MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMap(msg.boxedByteBoxedBooleanMap(), MessageCollectionItemType.BYTE, MessageCollectionItemType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMap(msg.boxedShortBoxedByteMap(), MessageCollectionItemType.SHORT, MessageCollectionItemType.BYTE))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeMap(msg.boxedIntBoxedShortMap(), MessageCollectionItemType.INT, MessageCollectionItemType.SHORT))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeMap(msg.boxedLongBoxedIntMap(), MessageCollectionItemType.LONG, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeMap(msg.boxedCharBoxedLongMap(), MessageCollectionItemType.CHAR, MessageCollectionItemType.LONG))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeMap(msg.boxedFloatBoxedCharMap(), MessageCollectionItemType.FLOAT, MessageCollectionItemType.CHAR))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeMap(msg.boxedDoubleBoxedFloatMap(), MessageCollectionItemType.DOUBLE, MessageCollectionItemType.FLOAT))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeMap(msg.messageBoxedDoubleMap(), MessageCollectionItemType.MSG, MessageCollectionItemType.DOUBLE))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** */
    @Override public boolean readFrom(Message m, ByteBuffer buf, MessageReader reader) {
        TestMapMessage msg = (TestMapMessage)m;

        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                msg.booleanArrayBoxedLongMap(reader.readMap(MessageCollectionItemType.BOOLEAN_ARR, MessageCollectionItemType.LONG, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.byteArrayBooleanArrayMap(reader.readMap(MessageCollectionItemType.BYTE_ARR, MessageCollectionItemType.BOOLEAN_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.shortArrayByteArrayMap(reader.readMap(MessageCollectionItemType.SHORT_ARR, MessageCollectionItemType.BYTE_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                msg.intArrayShortArrayMap(reader.readMap(MessageCollectionItemType.INT_ARR, MessageCollectionItemType.SHORT_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                msg.longArrayIntArrayMap(reader.readMap(MessageCollectionItemType.LONG_ARR, MessageCollectionItemType.INT_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                msg.charArrayLongArrayMap(reader.readMap(MessageCollectionItemType.CHAR_ARR, MessageCollectionItemType.LONG_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                msg.floatArrayCharArrayMap(reader.readMap(MessageCollectionItemType.FLOAT_ARR, MessageCollectionItemType.CHAR_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                msg.doubleArrayFloatArrayMap(reader.readMap(MessageCollectionItemType.DOUBLE_ARR, MessageCollectionItemType.FLOAT_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                msg.stringDoubleArrayMap(reader.readMap(MessageCollectionItemType.STRING, MessageCollectionItemType.DOUBLE_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                msg.uuidStringMap(reader.readMap(MessageCollectionItemType.UUID, MessageCollectionItemType.STRING, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                msg.bitSetUuidMap(reader.readMap(MessageCollectionItemType.BIT_SET, MessageCollectionItemType.UUID, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                msg.igniteUuidBitSetMap(reader.readMap(MessageCollectionItemType.IGNITE_UUID, MessageCollectionItemType.BIT_SET, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                msg.affTopVersionIgniteUuidMap(reader.readMap(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION, MessageCollectionItemType.IGNITE_UUID, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                msg.boxedBooleanAffTopVersionMap(reader.readMap(MessageCollectionItemType.BOOLEAN, MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                msg.boxedByteBoxedBooleanMap(reader.readMap(MessageCollectionItemType.BYTE, MessageCollectionItemType.BOOLEAN, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                msg.boxedShortBoxedByteMap(reader.readMap(MessageCollectionItemType.SHORT, MessageCollectionItemType.BYTE, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                msg.boxedIntBoxedShortMap(reader.readMap(MessageCollectionItemType.INT, MessageCollectionItemType.SHORT, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                msg.boxedLongBoxedIntMap(reader.readMap(MessageCollectionItemType.LONG, MessageCollectionItemType.INT, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                msg.boxedCharBoxedLongMap(reader.readMap(MessageCollectionItemType.CHAR, MessageCollectionItemType.LONG, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                msg.boxedFloatBoxedCharMap(reader.readMap(MessageCollectionItemType.FLOAT, MessageCollectionItemType.CHAR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                msg.boxedDoubleBoxedFloatMap(reader.readMap(MessageCollectionItemType.DOUBLE, MessageCollectionItemType.FLOAT, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                msg.messageBoxedDoubleMap(reader.readMap(MessageCollectionItemType.MSG, MessageCollectionItemType.DOUBLE, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }
}
