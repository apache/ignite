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

import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;

/**
 * Thin client payload input stream.
 */
class PayloadInputStream implements BinaryInputStream {
    /** Client channel. */
    private final ClientChannel ch;

    /** Delegate. */
    private final BinaryInputStream delegate;

    /**
     * Constructor.
     */
    PayloadInputStream(ClientChannel ch, byte[] payload) {
        delegate = new BinaryHeapInputStream(payload);
        this.ch = ch;
    }

    /**
     * Gets client channel.
     */
    public ClientChannel clientChannel() {
        return ch;
    }

    /** {@inheritDoc} */
    @Override public byte readByte() {
        return delegate.readByte();
    }

    /** {@inheritDoc} */
    @Override public byte[] readByteArray(int cnt) {
        return delegate.readByteArray(cnt);
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] arr, int off, int cnt) {
        return delegate.read(arr, off, cnt);
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() {
        return delegate.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public boolean[] readBooleanArray(int cnt) {
        return delegate.readBooleanArray(cnt);
    }

    /** {@inheritDoc} */
    @Override public short readShort() {
        return delegate.readShort();
    }

    /** {@inheritDoc} */
    @Override public short[] readShortArray(int cnt) {
        return delegate.readShortArray(cnt);
    }

    /** {@inheritDoc} */
    @Override public char readChar() {
        return delegate.readChar();
    }

    /** {@inheritDoc} */
    @Override public char[] readCharArray(int cnt) {
        return delegate.readCharArray(cnt);
    }

    /** {@inheritDoc} */
    @Override public int readInt() {
        return delegate.readInt();
    }

    /** {@inheritDoc} */
    @Override public int[] readIntArray(int cnt) {
        return delegate.readIntArray(cnt);
    }

    /** {@inheritDoc} */
    @Override public float readFloat() {
        return delegate.readFloat();
    }

    /** {@inheritDoc} */
    @Override public float[] readFloatArray(int cnt) {
        return delegate.readFloatArray(cnt);
    }

    /** {@inheritDoc} */
    @Override public long readLong() {
        return delegate.readLong();
    }

    /** {@inheritDoc} */
    @Override public long[] readLongArray(int cnt) {
        return delegate.readLongArray(cnt);
    }

    /** {@inheritDoc} */
    @Override public double readDouble() {
        return delegate.readDouble();
    }

    /** {@inheritDoc} */
    @Override public double[] readDoubleArray(int cnt) {
        return delegate.readDoubleArray(cnt);
    }

    /** {@inheritDoc} */
    @Override public int remaining() {
        return delegate.remaining();
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

    /** {@inheritDoc} */
    @Override public byte readBytePositioned(int pos) {
        return delegate.readBytePositioned(pos);
    }

    /** {@inheritDoc} */
    @Override public short readShortPositioned(int pos) {
        return delegate.readShortPositioned(pos);
    }

    /** {@inheritDoc} */
    @Override public int readIntPositioned(int pos) {
        return delegate.readIntPositioned(pos);
    }
}
