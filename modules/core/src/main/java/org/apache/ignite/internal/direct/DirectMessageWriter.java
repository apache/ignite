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
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Message writer implementation.
 */
public class DirectMessageWriter implements MessageWriter {
    /** Stream. */
    private final DirectByteBufferStream stream = new DirectByteBufferStream(null, null);

    /** State. */
    private final DirectMessageWriterState state = new DirectMessageWriterState();

    /** {@inheritDoc} */
    @Override public void setBuffer(ByteBuffer buf) {
        stream.setBuffer(buf);
    }

    /** {@inheritDoc} */
    @Override public boolean writeHeader(byte type, byte fieldCnt) {
        stream.writeByte(type);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeByte(String name, byte val) {
        stream.writeByte(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeShort(String name, short val) {
        stream.writeShort(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeInt(String name, int val) {
        stream.writeInt(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeLong(String name, long val) {
        stream.writeLong(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeFloat(String name, float val) {
        stream.writeFloat(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeDouble(String name, double val) {
        stream.writeDouble(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeChar(String name, char val) {
        stream.writeChar(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeBoolean(String name, boolean val) {
        stream.writeBoolean(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeByteArray(String name, @Nullable byte[] val) {
        stream.writeByteArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeByteArray(String name, byte[] val, long off, int len) {
        stream.writeByteArray(val, off, len);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeShortArray(String name, @Nullable short[] val) {
        stream.writeShortArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeIntArray(String name, @Nullable int[] val) {
        stream.writeIntArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeLongArray(String name, @Nullable long[] val) {
        stream.writeLongArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeFloatArray(String name, @Nullable float[] val) {
        stream.writeFloatArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeDoubleArray(String name, @Nullable double[] val) {
        stream.writeDoubleArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeCharArray(String name, @Nullable char[] val) {
        stream.writeCharArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeBooleanArray(String name, @Nullable boolean[] val) {
        stream.writeBooleanArray(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeString(String name, String val) {
        stream.writeString(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeBitSet(String name, BitSet val) {
        stream.writeBitSet(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeUuid(String name, UUID val) {
        stream.writeUuid(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeIgniteUuid(String name, IgniteUuid val) {
        stream.writeIgniteUuid(val);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean writeMessage(String name, @Nullable Message msg) {
        stream.writeMessage(msg, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <T> boolean writeObjectArray(String name, T[] arr, MessageCollectionItemType itemType) {
        stream.writeObjectArray(arr, itemType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <T> boolean writeCollection(String name, Collection<T> col, MessageCollectionItemType itemType) {
        stream.writeCollection(col, itemType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean writeMap(String name, Map<K, V> map, MessageCollectionItemType keyType,
        MessageCollectionItemType valType) {
        stream.writeMap(map, keyType, valType, this);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean isHeaderWritten() {
        return state.isTypeWritten();
    }

    /** {@inheritDoc} */
    @Override public void onHeaderWritten() {
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