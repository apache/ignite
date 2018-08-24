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

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.direct.state.DirectMessageState;
import org.apache.ignite.internal.direct.state.DirectMessageStateItem;
import org.apache.ignite.internal.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.direct.stream.v1.DirectByteBufferStreamImplV1;
import org.apache.ignite.internal.direct.stream.v2.DirectByteBufferStreamImplV2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.jetbrains.annotations.Nullable;

/**
 * Message reader implementation.
 */
public class DirectMessageReader implements MessageReader {
    /** State. */
    private final DirectMessageState<StateItem> state;

    /** Whether last field was fully read. */
    private boolean lastRead;

    /**
     * @param msgFactory Message factory.
     * @param protoVer Protocol version.
     */
    public DirectMessageReader(final MessageFactory msgFactory, final byte protoVer) {
        state = new DirectMessageState<>(StateItem.class, new IgniteOutClosure<StateItem>() {
            @Override public StateItem apply() {
                return new StateItem(msgFactory, protoVer);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void setBuffer(ByteBuffer buf) {
        state.item().stream.setBuffer(buf);
    }

    /** {@inheritDoc} */
    @Override public void setCurrentReadClass(Class<? extends Message> msgCls) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean beforeMessageRead() {
        return true;
    }

    /** {@inheritDoc}
     * @param msgCls*/
    @Override public boolean afterMessageRead(Class<? extends Message> msgCls) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public byte readByte(String name) {
        DirectByteBufferStream stream = state.item().stream;

        byte val = stream.readByte();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public short readShort(String name) {
        DirectByteBufferStream stream = state.item().stream;

        short val = stream.readShort();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public int readInt(String name) {
        DirectByteBufferStream stream = state.item().stream;

        int val = stream.readInt();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public int readInt(String name, int dflt) {
        return readInt(name);
    }

    /** {@inheritDoc} */
    @Override public long readLong(String name) {
        DirectByteBufferStream stream = state.item().stream;

        long val = stream.readLong();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String name) {
        DirectByteBufferStream stream = state.item().stream;

        float val = stream.readFloat();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String name) {
        DirectByteBufferStream stream = state.item().stream;

        double val = stream.readDouble();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public char readChar(String name) {
        DirectByteBufferStream stream = state.item().stream;

        char val = stream.readChar();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String name) {
        DirectByteBufferStream stream = state.item().stream;

        boolean val = stream.readBoolean();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        byte[] arr = stream.readByteArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        short[] arr = stream.readShortArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        int[] arr = stream.readIntArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        long[] arr = stream.readLongArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        float[] arr = stream.readFloatArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        double[] arr = stream.readDoubleArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        char[] arr = stream.readCharArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String name) {
        DirectByteBufferStream stream = state.item().stream;

        boolean[] arr = stream.readBooleanArray();

        lastRead = stream.lastFinished();

        return arr;
    }

    /** {@inheritDoc} */
    @Override public String readString(String name) {
        DirectByteBufferStream stream = state.item().stream;

        String val = stream.readString();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public BitSet readBitSet(String name) {
        DirectByteBufferStream stream = state.item().stream;

        BitSet val = stream.readBitSet();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public UUID readUuid(String name) {
        DirectByteBufferStream stream = state.item().stream;

        UUID val = stream.readUuid();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid readIgniteUuid(String name) {
        DirectByteBufferStream stream = state.item().stream;

        IgniteUuid val = stream.readIgniteUuid();

        lastRead = stream.lastFinished();

        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T extends Message> T readMessage(String name) {
        DirectByteBufferStream stream = state.item().stream;

        T msg = stream.readMessage(this);

        lastRead = stream.lastFinished();

        return msg;
    }

    /** {@inheritDoc} */
    @Override public <T> T[] readObjectArray(String name, MessageCollectionItemType itemType, Class<T> itemCls) {
        DirectByteBufferStream stream = state.item().stream;

        T[] msg = stream.readObjectArray(itemType, itemCls, this);

        lastRead = stream.lastFinished();

        return msg;
    }

    /** {@inheritDoc} */
    @Override public <C extends Collection<?>> C readCollection(String name, MessageCollectionItemType itemType) {
        DirectByteBufferStream stream = state.item().stream;

        C col = stream.readCollection(itemType, this);

        lastRead = stream.lastFinished();

        return col;
    }

    /** {@inheritDoc} */
    @Override public <M extends Map<?, ?>> M readMap(String name, MessageCollectionItemType keyType,
        MessageCollectionItemType valType, boolean linked) {
        DirectByteBufferStream stream = state.item().stream;

        M map = stream.readMap(keyType, valType, linked, this);

        lastRead = stream.lastFinished();

        return map;
    }

    /** {@inheritDoc} */
    @Override public boolean isLastRead() {
        return lastRead;
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
    @Override public void beforeInnerMessageRead() {
        state.forward();
    }

    /** {@inheritDoc} */
    @Override public void afterInnerMessageRead(boolean finished) {
        state.backward(finished);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        state.reset();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DirectMessageReader.class, this);
    }

    /**
     */
    private static class StateItem implements DirectMessageStateItem {
        /** Stream. */
        private final DirectByteBufferStream stream;

        /** State. */
        private int state;

        /**
         * @param msgFactory Message factory.
         * @param protoVer Protocol version.
         */
        public StateItem(MessageFactory msgFactory, byte protoVer) {
            switch (protoVer) {
                case 1:
                    stream = new DirectByteBufferStreamImplV1(msgFactory);

                    break;

                case 2:
                    stream = new DirectByteBufferStreamImplV2(msgFactory);

                    break;

                default:
                    throw new IllegalStateException("Invalid protocol version: " + protoVer);
            }
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            state = 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StateItem.class, this);
        }
    }
}
