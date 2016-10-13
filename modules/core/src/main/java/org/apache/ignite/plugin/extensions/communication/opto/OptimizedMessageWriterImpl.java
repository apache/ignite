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

package org.apache.ignite.plugin.extensions.communication.opto;

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Optimized message writer implementation.
 */
public class OptimizedMessageWriterImpl implements OptimizedMessageWriter {
    /** State. */
    private final OptimizedMessageState state;

    /** Current buffer. */
    private ByteBuffer buf;

    /**
     * Constructor.
     *
     * @param state State.
     */
    public OptimizedMessageWriterImpl(OptimizedMessageState state) {
        this.state = state;

        buf = state.buffer();
    }

    /** {@inheritDoc} */
    @Override public void writeHeader(byte type, byte fieldCnt) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val, long off, int len) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(short[] val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(int[] val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(long[] val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(float[] val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(double[] val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(char[] val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(boolean[] val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeString(String val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeBitSet(BitSet val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(UUID val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeIgniteUuid(IgniteUuid val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public void writeMessage(Message val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public <T> void writeObjectArray(T[] arr, MessageCollectionItemType itemType) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(Collection<T> col, MessageCollectionItemType itemType) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(Map<K, V> map, MessageCollectionItemType keyType,
        MessageCollectionItemType valType) {
        // TODO
    }
}
