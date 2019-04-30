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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

/**
 * Thin client payload output stream.
 */
class PayloadOutputStream implements BinaryOutputStream {
    /** Initial output stream buffer capacity. */
    private static final int INITIAL_BUFFER_CAPACITY = 1024;

    /** Client channel. */
    private final ClientChannel ch;

    /** Delegate. */
    private final BinaryOutputStream delegate;

    /**
     * Constructor.
     */
    PayloadOutputStream(ClientChannel ch) {
        delegate = new BinaryHeapOutputStream(INITIAL_BUFFER_CAPACITY);
        this.ch = ch;
    }

    /**
     * Gets client channel.
     */
    public ClientChannel clientChannel() {
        return ch;
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) {
        delegate.writeByte(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val) {
        delegate.writeByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) {
        delegate.writeBoolean(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(boolean[] val) {
        delegate.writeBooleanArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) {
        delegate.writeShort(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(short[] val) {
        delegate.writeShortArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) {
        delegate.writeChar(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(char[] val) {
        delegate.writeCharArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) {
        delegate.writeInt(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int pos, short val) {
        delegate.writeShort(pos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int pos, int val) {
        delegate.writeInt(pos, val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(int[] val) {
        delegate.writeIntArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) {
        delegate.writeFloat(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(float[] val) {
        delegate.writeFloatArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) {
        delegate.writeLong(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(long[] val) {
        delegate.writeLongArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) {
        delegate.writeDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(double[] val) {
        delegate.writeDoubleArray(val);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] arr, int off, int len) {
        delegate.write(arr, off, len);
    }

    /** {@inheritDoc} */
    @Override public void write(long addr, int cnt) {
        delegate.write(addr, cnt);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        delegate.close();
    }

    /** {@inheritDoc} */
    @Override public void unsafePosition(int pos) {
        delegate.unsafePosition(pos);
    }

    /** {@inheritDoc} */
    @Override public void unsafeEnsure(int cap) {
        delegate.unsafeEnsure(cap);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteByte(byte val) {
        delegate.unsafeWriteByte(val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteBoolean(boolean val) {
        delegate.unsafeWriteBoolean(val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteShort(short val) {
        delegate.unsafeWriteShort(val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteShort(int pos, short val) {
        delegate.unsafeWriteShort(pos, val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteChar(char val) {
        delegate.unsafeWriteChar(val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteInt(int val) {
        delegate.unsafeWriteInt(val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteInt(int pos, int val) {
        delegate.unsafeWriteInt(pos, val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteLong(long val) {
        delegate.unsafeWriteLong(val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteFloat(float val) {
        delegate.unsafeWriteFloat(val);
    }

    /** {@inheritDoc} */
    @Override public void unsafeWriteDouble(double val) {
        delegate.unsafeWriteDouble(val);
    }

    /** {@inheritDoc} */
    @Override public int position() {
        return delegate.position();
    }

    /** {@inheritDoc} */
    @Override public void position(int pos) {
        delegate.position(pos);
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return delegate.array();
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        return delegate.arrayCopy();
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return delegate.offheapPointer();
    }

    /** {@inheritDoc} */
    @Override public long rawOffheapPointer() {
        return delegate.rawOffheapPointer();
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        return delegate.hasArray();
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return delegate.capacity();
    }
}
