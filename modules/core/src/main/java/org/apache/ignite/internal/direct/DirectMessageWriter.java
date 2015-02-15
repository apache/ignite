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

import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 * Message writer implementation.
 */
public class DirectMessageWriter implements MessageWriter {
    /** Stream. */
    private final DirectByteBufferStream stream = new DirectByteBufferStream(null);

    /** State. */
    private final MessageWriterState state = new MessageWriterState();

    /** {@inheritDoc} */
    @Override public void setBuffer(ByteBuffer buf) {
        stream.setBuffer(buf);
    }

    /** {@inheritDoc} */
    @Override public boolean writeMessageType(byte msgType) {
        stream.writeByte(msgType);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeField(String name, Object val, MessageFieldType type) {
        switch (type) {
            case BYTE:
                stream.writeByte((byte)val);

                break;

            case SHORT:
                stream.writeShort((short)val);

                break;

            case INT:
                stream.writeInt((int)val);

                break;

            case LONG:
                stream.writeLong((long)val);

                break;

            case FLOAT:
                stream.writeFloat((float)val);

                break;

            case DOUBLE:
                stream.writeDouble((double)val);

                break;

            case CHAR:
                stream.writeChar((char)val);

                break;

            case BOOLEAN:
                stream.writeBoolean((boolean)val);

                break;

            case BYTE_ARR:
                stream.writeByteArray((byte[])val);

                break;

            case SHORT_ARR:
                stream.writeShortArray((short[])val);

                break;

            case INT_ARR:
                stream.writeIntArray((int[])val);

                break;

            case LONG_ARR:
                stream.writeLongArray((long[])val);

                break;

            case FLOAT_ARR:
                stream.writeFloatArray((float[])val);

                break;

            case DOUBLE_ARR:
                stream.writeDoubleArray((double[])val);

                break;

            case CHAR_ARR:
                stream.writeCharArray((char[])val);

                break;

            case BOOLEAN_ARR:
                stream.writeBooleanArray((boolean[])val);

                break;

            case STRING:
                stream.writeString((String)val);

                break;

            case BIT_SET:
                stream.writeBitSet((BitSet)val);

                break;

            case UUID:
                stream.writeUuid((UUID)val);

                break;

            case IGNITE_UUID:
                stream.writeIgniteUuid((IgniteUuid)val);

                break;

            case MSG:
                stream.writeMessage((MessageAdapter)val, this);

                break;

            default:
                throw new IllegalStateException("Unknown field type: " + type);
        }

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <T> boolean writeArrayField(String name, T[] arr, MessageFieldType itemType) {
        stream.writeObjectArray(arr, itemType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <T> boolean writeCollectionField(String name, Collection<T> col, MessageFieldType itemType) {
        stream.writeCollection(col, itemType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean writeMapField(String name, Map<K, V> map, MessageFieldType keyType,
        MessageFieldType valType) {
        stream.writeMap(map, keyType, valType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean isTypeWritten() {
        return state.isTypeWritten();
    }

    /** {@inheritDoc} */
    @Override public void onTypeWritten() {
        state.onTypeWritten();
    }

    /** {@inheritDoc} */
    @Override public int state() {
        return state.state();
    }

    /** {@inheritDoc} */
    @Override public void incrementState() {
        state.incrementState();
    }

    /** {@inheritDoc} */
    @Override public void beforeInnerMessageWrite() {
        state.beforeInnerMessageWrite();
    }

    /** {@inheritDoc} */
    @Override public void afterInnerMessageWrite(boolean finished) {
        state.afterInnerMessageWrite(finished);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        state.reset();
    }
}
