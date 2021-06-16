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

package org.apache.ignite.internal.network.direct;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.internal.network.direct.state.DirectMessageState;
import org.apache.ignite.internal.network.direct.state.DirectMessageStateItem;
import org.apache.ignite.internal.network.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.network.direct.stream.DirectByteBufferStreamImplV1;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.jetbrains.annotations.Nullable;

public class DirectMessageWriter implements MessageWriter {
    /** State. */
    private final DirectMessageState<StateItem> state;

    /**
     * @param serializationRegistry Serialization registry.
     * @param protoVer Protocol version.
     */
    public DirectMessageWriter(MessageSerializationRegistry serializationRegistry, byte protoVer) {
        state = new DirectMessageState<>(StateItem.class, () -> new StateItem(serializationRegistry, protoVer));
    }

    /** {@inheritDoc} */
    @Override public void setBuffer(ByteBuffer buf) {
        state.item().stream.setBuffer(buf);
    }

    /** {@inheritDoc} */
    @Override public void setCurrentWriteClass(Class<? extends NetworkMessage> msgCls) {
        // No-op.
    }

    /** {@inheritDoc} */
    // TODO: compress the header https://issues.apache.org/jira/browse/IGNITE-14818
    @Override public boolean writeHeader(short groupType, short messageType, byte fieldCnt) {
        DirectByteBufferStream stream = state.item().stream;

        // first part of the header might have already been sent in a previous write attempt
        if (!state.item().partialHdrWritten) {
            stream.writeShort(groupType);

            if (stream.lastFinished())
                state.item().partialHdrWritten = true;
            else
                return false;
        }

        stream.writeShort(messageType);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeByte(String name, byte val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeByte(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeShort(String name, short val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeShort(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeInt(String name, int val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeInt(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeLong(String name, long val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeLong(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeFloat(String name, float val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeFloat(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeDouble(String name, double val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeDouble(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeChar(String name, char val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeChar(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeBoolean(String name, boolean val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeBoolean(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeByteArray(String name, @Nullable byte[] val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeByteArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeByteArray(String name, byte[] val, long off, int len) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeByteArray(val, off, len);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeShortArray(String name, @Nullable short[] val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeShortArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeIntArray(String name, @Nullable int[] val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeIntArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeLongArray(String name, @Nullable long[] val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeLongArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeLongArray(String name, long[] val, int len) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeLongArray(val, len);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeFloatArray(String name, @Nullable float[] val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeFloatArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeDoubleArray(String name, @Nullable double[] val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeDoubleArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeCharArray(String name, @Nullable char[] val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeCharArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeBooleanArray(String name, @Nullable boolean[] val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeBooleanArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeString(String name, String val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeString(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeBitSet(String name, BitSet val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeBitSet(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeUuid(String name, UUID val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeUuid(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeIgniteUuid(String name, IgniteUuid val) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeIgniteUuid(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeMessage(String name, @Nullable NetworkMessage msg) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeMessage(msg, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <T> boolean writeObjectArray(String name, T[] arr, MessageCollectionItemType itemType) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeObjectArray(arr, itemType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <T> boolean writeCollection(String name, Collection<T> col, MessageCollectionItemType itemType) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeCollection(col, itemType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean writeMap(String name, Map<K, V> map, MessageCollectionItemType keyType,
        MessageCollectionItemType valType) {
        DirectByteBufferStream stream = state.item().stream;

        stream.writeMap(map, keyType, valType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean isHeaderWritten() {
        return state.item().hdrWritten;
    }

    /** {@inheritDoc} */
    @Override public void onHeaderWritten() {
        state.item().hdrWritten = true;
    }

    /** {@inheritDoc} */
    @Override public int state() {
        return state.item().state;
    }

    /** {@inheritDoc} */
    @Override public void incrementState() {
        state.item().state++;
    }

    /** {@inheritDoc} */
    @Override public void beforeInnerMessageWrite() {
        state.forward();
    }

    /** {@inheritDoc} */
    @Override public void afterInnerMessageWrite(boolean finished) {
        state.backward(finished);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        state.reset();
    }

    /**
     */
    private static class StateItem implements DirectMessageStateItem {
        /** */
        private final DirectByteBufferStream stream;

        /** */
        private int state;

        /**
         * Flag indicating that the first part of the message header has been written.
         */
        private boolean partialHdrWritten;

        /**
         * Flag indicating that the whole message header has been written.
         */
        private boolean hdrWritten;

        /**
         * @param registry Serialization registry.
         * @param protoVer Protocol version.
         */
        StateItem(MessageSerializationRegistry registry, byte protoVer) {
            switch (protoVer) {
                case 1:
                    stream = new DirectByteBufferStreamImplV1(registry);

                    break;

                default:
                    throw new IllegalStateException("Invalid protocol version: " + protoVer);
            }
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            state = 0;
            partialHdrWritten = false;
            hdrWritten = false;
        }
    }
}
