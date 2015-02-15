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

package org.apache.ignite.internal.direct;

import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;
import java.util.*;

/**
 * Message reader implementation.
 */
public class DirectMessageReader implements MessageReader {
    /** Stream. */
    private final DirectByteBufferStream stream;

    /** Whether last field was fully read. */
    private boolean lastRead;

    /**
     * @param msgFactory Message factory.
     */
    public DirectMessageReader(MessageFactory msgFactory) {
        this.stream = new DirectByteBufferStream(msgFactory);
    }

    /** {@inheritDoc} */
    @Override public void setBuffer(ByteBuffer buf) {
        stream.setBuffer(buf);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T readField(String name, MessageFieldType type) {
        Object val;

        switch (type) {
            case BYTE:
                val = stream.readByte();

                break;

            case SHORT:
                val = stream.readShort();

                break;

            case INT:
                val = stream.readInt();

                break;

            case LONG:
                val = stream.readLong();

                break;

            case FLOAT:
                val = stream.readFloat();

                break;

            case DOUBLE:
                val = stream.readDouble();

                break;

            case CHAR:
                val = stream.readChar();

                break;

            case BOOLEAN:
                val = stream.readBoolean();

                break;

            case BYTE_ARR:
                val = stream.readByteArray();

                break;

            case SHORT_ARR:
                val = stream.readShortArray();

                break;

            case INT_ARR:
                val = stream.readIntArray();

                break;

            case LONG_ARR:
                val = stream.readLongArray();

                break;

            case FLOAT_ARR:
                val = stream.readFloatArray();

                break;

            case DOUBLE_ARR:
                val = stream.readDoubleArray();

                break;

            case CHAR_ARR:
                val = stream.readCharArray();

                break;

            case BOOLEAN_ARR:
                val = stream.readBooleanArray();

                break;

            case STRING:
                val = stream.readString();

                break;

            case BIT_SET:
                val = stream.readBitSet();

                break;

            case UUID:
                val = stream.readUuid();

                break;

            case IGNITE_UUID:
                val = stream.readIgniteUuid();

                break;

            case MSG:
                val = stream.readMessage();

                break;

            default:
                throw new IllegalStateException("Unknown field type: " + type);
        }

        lastRead = stream.lastFinished();

        return (T)val;
    }

    /** {@inheritDoc} */
    @Override public <T> T[] readArrayField(String name, MessageFieldType itemType, Class<T> itemCls) {
        T[] msg = stream.readObjectArray(itemType, itemCls);

        lastRead = stream.lastFinished();

        return msg;
    }

    /** {@inheritDoc} */
    @Override public <C extends Collection<?>> C readCollectionField(String name, MessageFieldType itemType) {
        C col = stream.readCollection(itemType);

        lastRead = stream.lastFinished();

        return col;
    }

    /** {@inheritDoc} */
    @Override public <M extends Map<?, ?>> M readMapField(String name, MessageFieldType keyType,
        MessageFieldType valType, boolean linked) {
        M map = stream.readMap(keyType, valType, linked);

        lastRead = stream.lastFinished();

        return map;
    }

    /** {@inheritDoc} */
    @Override public boolean isLastRead() {
        return lastRead;
    }
}
