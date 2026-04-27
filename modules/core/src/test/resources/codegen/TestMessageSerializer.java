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

import org.apache.ignite.internal.TestMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageArrayType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestMessageSerializer implements MessageSerializer<TestMessage> {
    /** */
    private final static MessageArrayType intMatrixCollDesc = new MessageArrayType(new MessageItemType(MessageCollectionItemType.INT_ARR), int[].class);
    /** */
    private final static MessageArrayType strArrCollDesc = new MessageArrayType(new MessageItemType(MessageCollectionItemType.STRING), String.class);
    /** */
    private final static MessageArrayType verArrCollDesc = new MessageArrayType(new MessageItemType(MessageCollectionItemType.MSG), GridCacheVersion.class);

    /** */
    @Override public boolean writeTo(TestMessage msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt(msg.id))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByteArray(msg.byteArr))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeString(msg.str))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeObjectArray(msg.strArr, strArrCollDesc))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeObjectArray(msg.intMatrix, intMatrixCollDesc))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage(msg.ver))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeObjectArray(msg.verArr, verArrCollDesc))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeUuid(msg.uuid))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeIgniteUuid(msg.ignUuid))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeAffinityTopologyVersion(msg.topVer))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeBitSet(msg.bitSet))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeString(msg.field))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeKeyCacheObject(msg.keyCacheObject))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeCacheObject(msg.cacheObject))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeGridLongList(msg.gridLongList))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** */
    @Override public boolean readFrom(TestMessage msg, MessageReader reader) {
        switch (reader.state()) {
            case 0:
                msg.id = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                msg.byteArr = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg.str = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                msg.strArr = reader.readObjectArray(strArrCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                msg.intMatrix = reader.readObjectArray(intMatrixCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                msg.ver = reader.readMessage();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                msg.verArr = reader.readObjectArray(verArrCollDesc);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                msg.uuid = reader.readUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                msg.ignUuid = reader.readIgniteUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                msg.topVer = reader.readAffinityTopologyVersion();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                msg.bitSet = reader.readBitSet();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                msg.field = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                msg.keyCacheObject = reader.readKeyCacheObject();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                msg.cacheObject = reader.readCacheObject();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                msg.gridLongList = reader.readGridLongList();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return true;
    }
}