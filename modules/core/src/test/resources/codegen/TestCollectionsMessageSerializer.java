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
import org.apache.ignite.internal.TestCollectionsMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestCollectionsMessageSerializer implements MessageSerializer {
    /** */
    @Override public boolean writeTo(Message m, ByteBuffer buf, MessageWriter writer) {
        TestCollectionsMessage msg = (TestCollectionsMessage)m;

        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection(msg.booleanArrayList(), MessageCollectionItemType.BOOLEAN_ARR))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection(msg.byteArrayList(), MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection(msg.shortArrayList(), MessageCollectionItemType.SHORT_ARR))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeCollection(msg.intArrayList(), MessageCollectionItemType.INT_ARR))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeCollection(msg.longArrayList(), MessageCollectionItemType.LONG_ARR))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection(msg.charArrayList(), MessageCollectionItemType.CHAR_ARR))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollection(msg.floatArrayList(), MessageCollectionItemType.FLOAT_ARR))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeCollection(msg.doubleArrayList(), MessageCollectionItemType.DOUBLE_ARR))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeCollection(msg.stringList(), MessageCollectionItemType.STRING))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection(msg.uuidList(), MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeCollection(msg.bitSetList(), MessageCollectionItemType.BIT_SET))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeCollection(msg.igniteUuidList(), MessageCollectionItemType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeCollection(msg.affTopVersionList(), MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeCollection(msg.boxedBooleanList(), MessageCollectionItemType.BOOLEAN))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection(msg.boxedByteList(), MessageCollectionItemType.BYTE))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollection(msg.boxedShortList(), MessageCollectionItemType.SHORT))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeCollection(msg.boxedIntList(), MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeCollection(msg.boxedLongList(), MessageCollectionItemType.LONG))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeCollection(msg.boxedCharList(), MessageCollectionItemType.CHAR))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeCollection(msg.boxedFloatList(), MessageCollectionItemType.FLOAT))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeCollection(msg.boxedDoubleList(), MessageCollectionItemType.DOUBLE))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeCollection(msg.messageList(), MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeCollection(msg.gridLongListList(), MessageCollectionItemType.GRID_LONG_LIST))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** */
    @Override public boolean readFrom(Message m, ByteBuffer buf, MessageReader reader) {
        TestCollectionsMessage msg = (TestCollectionsMessage)m;

        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                msg.booleanArrayList(reader.readCollection(MessageCollectionItemType.BOOLEAN_ARR));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.byteArrayList(reader.readCollection(MessageCollectionItemType.BYTE_ARR));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.shortArrayList(reader.readCollection(MessageCollectionItemType.SHORT_ARR));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                msg.intArrayList(reader.readCollection(MessageCollectionItemType.INT_ARR));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                msg.longArrayList(reader.readCollection(MessageCollectionItemType.LONG_ARR));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                msg.charArrayList(reader.readCollection(MessageCollectionItemType.CHAR_ARR));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                msg.floatArrayList(reader.readCollection(MessageCollectionItemType.FLOAT_ARR));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                msg.doubleArrayList(reader.readCollection(MessageCollectionItemType.DOUBLE_ARR));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                msg.stringList(reader.readCollection(MessageCollectionItemType.STRING));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                msg.uuidList(reader.readCollection(MessageCollectionItemType.UUID));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                msg.bitSetList(reader.readCollection(MessageCollectionItemType.BIT_SET));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                msg.igniteUuidList(reader.readCollection(MessageCollectionItemType.IGNITE_UUID));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                msg.affTopVersionList(reader.readCollection(MessageCollectionItemType.AFFINITY_TOPOLOGY_VERSION));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                msg.boxedBooleanList(reader.readCollection(MessageCollectionItemType.BOOLEAN));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                msg.boxedByteList(reader.readCollection(MessageCollectionItemType.BYTE));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                msg.boxedShortList(reader.readCollection(MessageCollectionItemType.SHORT));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                msg.boxedIntList(reader.readCollection(MessageCollectionItemType.INT));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 17:
                msg.boxedLongList(reader.readCollection(MessageCollectionItemType.LONG));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                msg.boxedCharList(reader.readCollection(MessageCollectionItemType.CHAR));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                msg.boxedFloatList(reader.readCollection(MessageCollectionItemType.FLOAT));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                msg.boxedDoubleList(reader.readCollection(MessageCollectionItemType.DOUBLE));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                msg.messageList(reader.readCollection(MessageCollectionItemType.MSG));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                msg.gridLongListList(reader.readCollection(MessageCollectionItemType.GRID_LONG_LIST));

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }
}
