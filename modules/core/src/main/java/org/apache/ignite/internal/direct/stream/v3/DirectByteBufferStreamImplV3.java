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

package org.apache.ignite.internal.direct.stream.v3;

import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.direct.stream.v2.DirectByteBufferStreamImplV2;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class DirectByteBufferStreamImplV3 extends DirectByteBufferStreamImplV2 {
    /** */
    private byte topVerState;

    /** */
    private long topVerMajor;

    /** */
    private int topVerMinor;

    /**
     * @param msgFactory Message factory.
     */
    public DirectByteBufferStreamImplV3(MessageFactory msgFactory) {
        super(msgFactory);
    }

    /** {@inheritDoc} */
    @Override public void writeAffinityTopologyVersion(AffinityTopologyVersion val) {
        if (val != null) {
            switch (topVerState) {
                case 0:
                    writeInt(val.minorTopologyVersion());

                    if (!lastFinished)
                        return;

                    topVerState++;

                case 1:
                    writeLong(val.topologyVersion());

                    if (!lastFinished)
                        return;

                    topVerState = 0;
            }
        }
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion readAffinityTopologyVersion() {
        switch (topVerState) {
            case 0:
                topVerMinor = readInt();

                if (!lastFinished || topVerMinor == -1)
                    return null;

                topVerState++;

            case 1:
                topVerMajor = readLong();

                if (!lastFinished)
                    return null;

                topVerState = 0;
        }

        return new AffinityTopologyVersion(topVerMajor, topVerMinor);
    }

    /** {@inheritDoc} */
    @Override protected void write(MessageCollectionItemType type, Object val, MessageWriter writer) {
        switch (type) {
            case BYTE:
                writeByte((Byte)val);

                break;

            case SHORT:
                writeShort((Short)val);

                break;

            case INT:
                writeInt((Integer)val);

                break;

            case LONG:
                writeLong((Long)val);

                break;

            case FLOAT:
                writeFloat((Float)val);

                break;

            case DOUBLE:
                writeDouble((Double)val);

                break;

            case CHAR:
                writeChar((Character)val);

                break;

            case BOOLEAN:
                writeBoolean((Boolean)val);

                break;

            case BYTE_ARR:
                writeByteArray((byte[])val);

                break;

            case SHORT_ARR:
                writeShortArray((short[])val);

                break;

            case INT_ARR:
                writeIntArray((int[])val);

                break;

            case LONG_ARR:
                writeLongArray((long[])val);

                break;

            case FLOAT_ARR:
                writeFloatArray((float[])val);

                break;

            case DOUBLE_ARR:
                writeDoubleArray((double[])val);

                break;

            case CHAR_ARR:
                writeCharArray((char[])val);

                break;

            case BOOLEAN_ARR:
                writeBooleanArray((boolean[])val);

                break;

            case STRING:
                writeString((String)val);

                break;

            case BIT_SET:
                writeBitSet((BitSet)val);

                break;

            case UUID:
                writeUuid((UUID)val);

                break;

            case IGNITE_UUID:
                writeIgniteUuid((IgniteUuid)val);

                break;

            case AFFINITY_TOPOLOGY_VERSION:
                writeAffinityTopologyVersion((AffinityTopologyVersion)val);

                break;
            case MSG:
                try {
                    if (val != null)
                        writer.beforeInnerMessageWrite();

                    writeMessage((Message)val, writer);
                }
                finally {
                    if (val != null)
                        writer.afterInnerMessageWrite(lastFinished);
                }

                break;

            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /** {@inheritDoc} */
    @Override protected Object read(MessageCollectionItemType type, MessageReader reader) {
        switch (type) {
            case BYTE:
                return readByte();

            case SHORT:
                return readShort();

            case INT:
                return readInt();

            case LONG:
                return readLong();

            case FLOAT:
                return readFloat();

            case DOUBLE:
                return readDouble();

            case CHAR:
                return readChar();

            case BOOLEAN:
                return readBoolean();

            case BYTE_ARR:
                return readByteArray();

            case SHORT_ARR:
                return readShortArray();

            case INT_ARR:
                return readIntArray();

            case LONG_ARR:
                return readLongArray();

            case FLOAT_ARR:
                return readFloatArray();

            case DOUBLE_ARR:
                return readDoubleArray();

            case CHAR_ARR:
                return readCharArray();

            case BOOLEAN_ARR:
                return readBooleanArray();

            case STRING:
                return readString();

            case BIT_SET:
                return readBitSet();

            case UUID:
                return readUuid();

            case IGNITE_UUID:
                return readIgniteUuid();

            case AFFINITY_TOPOLOGY_VERSION:
                return readAffinityTopologyVersion();

            case MSG:
                return readMessage(reader);

            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }
}
