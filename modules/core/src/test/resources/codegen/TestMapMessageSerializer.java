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
                if (!writer.writeMap(msg.booleanArrayMap(), MessageCollectionItemType.BOOLEAN_ARR, MessageCollectionItemType.BOOLEAN_ARR))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap(msg.byteArrayMap(), MessageCollectionItemType.BYTE_ARR, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMap(msg.shortArrayMap(), MessageCollectionItemType.SHORT_ARR, MessageCollectionItemType.SHORT_ARR))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMap(msg.intArrayMap(), MessageCollectionItemType.INT_ARR, MessageCollectionItemType.INT_ARR))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap(msg.longArrayMap(), MessageCollectionItemType.LONG_ARR, MessageCollectionItemType.LONG_ARR))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMap(msg.charArrayMap(), MessageCollectionItemType.CHAR_ARR, MessageCollectionItemType.CHAR_ARR))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMap(msg.floatArrayMap(), MessageCollectionItemType.FLOAT_ARR, MessageCollectionItemType.FLOAT_ARR))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMap(msg.doubleArrayMap(), MessageCollectionItemType.DOUBLE_ARR, MessageCollectionItemType.DOUBLE_ARR))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMap(msg.stringMap(), MessageCollectionItemType.STRING, MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMap(msg.uuidMap(), MessageCollectionItemType.UUID, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeMap(msg.bitSetMap(), MessageCollectionItemType.BIT_SET, MessageCollectionItemType.BIT_SET))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMap(msg.igniteUuidMap(), MessageCollectionItemType.IGNITE_UUID, MessageCollectionItemType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeMap(msg.affTopVersionMap(), MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION, MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMap(msg.boxedBooleanMap(), MessageCollectionItemType.BOOLEAN, MessageCollectionItemType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMap(msg.boxedByteMap(), MessageCollectionItemType.BYTE, MessageCollectionItemType.BYTE))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeMap(msg.boxedShortMap(), MessageCollectionItemType.SHORT, MessageCollectionItemType.SHORT))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeMap(msg.boxedIntMap(), MessageCollectionItemType.INT, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeMap(msg.boxedLongMap(), MessageCollectionItemType.LONG, MessageCollectionItemType.LONG))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeMap(msg.boxedCharMap(), MessageCollectionItemType.CHAR, MessageCollectionItemType.CHAR))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeMap(msg.boxedFloatMap(), MessageCollectionItemType.FLOAT, MessageCollectionItemType.FLOAT))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeMap(msg.boxedDoubleMap(), MessageCollectionItemType.DOUBLE, MessageCollectionItemType.DOUBLE))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeMap(msg.messageMap(), MessageCollectionItemType.MSG, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeMap(msg.linkedMap(), MessageCollectionItemType.LONG, MessageCollectionItemType.LONG))
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
                msg.booleanArrayMap(reader.readMap(MessageCollectionItemType.BOOLEAN_ARR, MessageCollectionItemType.BOOLEAN_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.byteArrayMap(reader.readMap(MessageCollectionItemType.BYTE_ARR, MessageCollectionItemType.BYTE_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.shortArrayMap(reader.readMap(MessageCollectionItemType.SHORT_ARR, MessageCollectionItemType.SHORT_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                msg.intArrayMap(reader.readMap(MessageCollectionItemType.INT_ARR, MessageCollectionItemType.INT_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                msg.longArrayMap(reader.readMap(MessageCollectionItemType.LONG_ARR, MessageCollectionItemType.LONG_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                msg.charArrayMap(reader.readMap(MessageCollectionItemType.CHAR_ARR, MessageCollectionItemType.CHAR_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                msg.floatArrayMap(reader.readMap(MessageCollectionItemType.FLOAT_ARR, MessageCollectionItemType.FLOAT_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                msg.doubleArrayMap(reader.readMap(MessageCollectionItemType.DOUBLE_ARR, MessageCollectionItemType.DOUBLE_ARR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                msg.stringMap(reader.readMap(MessageCollectionItemType.STRING, MessageCollectionItemType.STRING, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                msg.uuidMap(reader.readMap(MessageCollectionItemType.UUID, MessageCollectionItemType.UUID, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                msg.bitSetMap(reader.readMap(MessageCollectionItemType.BIT_SET, MessageCollectionItemType.BIT_SET, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                msg.igniteUuidMap(reader.readMap(MessageCollectionItemType.IGNITE_UUID, MessageCollectionItemType.IGNITE_UUID, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                msg.affTopVersionMap(reader.readMap(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION, MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                msg.boxedBooleanMap(reader.readMap(MessageCollectionItemType.BOOLEAN, MessageCollectionItemType.BOOLEAN, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                msg.boxedByteMap(reader.readMap(MessageCollectionItemType.BYTE, MessageCollectionItemType.BYTE, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                msg.boxedShortMap(reader.readMap(MessageCollectionItemType.SHORT, MessageCollectionItemType.SHORT, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                msg.boxedIntMap(reader.readMap(MessageCollectionItemType.INT, MessageCollectionItemType.INT, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                msg.boxedLongMap(reader.readMap(MessageCollectionItemType.LONG, MessageCollectionItemType.LONG, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                msg.boxedCharMap(reader.readMap(MessageCollectionItemType.CHAR, MessageCollectionItemType.CHAR, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                msg.boxedFloatMap(reader.readMap(MessageCollectionItemType.FLOAT, MessageCollectionItemType.FLOAT, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                msg.boxedDoubleMap(reader.readMap(MessageCollectionItemType.DOUBLE, MessageCollectionItemType.DOUBLE, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                msg.messageMap(reader.readMap(MessageCollectionItemType.MSG, MessageCollectionItemType.MSG, false));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                msg.linkedMap(reader.readMap(MessageCollectionItemType.LONG, MessageCollectionItemType.LONG, true));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }
}
