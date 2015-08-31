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

package org.apache.ignite.internal.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.util.io.GridUnsafeDataInput;
import org.apache.ignite.internal.util.io.GridUnsafeDataOutput;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Re-sizable array implementation of the byte list (eliminating auto-boxing of primitive byte type).
 */
public class GridByteArrayList implements Message, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** List byte data. */
    @GridToStringExclude
    private byte[] data;

    /** List's size. */
    private int size;

    /**
     * No-op constructor that creates uninitialized list. This method is meant
     * to by used only by {@link Externalizable} interface.
     */
    public GridByteArrayList() {
        // No-op.
    }

    /**
     * Creates empty list with the specified initial capacity.
     *
     * @param cap Initial capacity.
     */
    public GridByteArrayList(int cap) {
        assert cap > 0;

        data = new byte[cap];
    }

    /**
     * Wraps existing array into byte array list.
     *
     * @param data Array to wrap.
     * @param size Size of data inside of array.
     */
    public GridByteArrayList(byte[] data, int size) {
        assert data != null;
        assert size > 0;

        this.data = data;
        this.size = size;
    }

    /**
     * Wraps existing array into byte array list.
     *
     * @param data Array to wrap.
     */
    public GridByteArrayList(byte[] data) {
        assert data != null;

        this.data = data;

        size = data.length;
    }

    /**
     * Resets byte array to empty. Note that this method simply resets the size
     * as there is no need to reset every byte in the array.
     */
    public void reset() {
        size = 0;
    }

    /**
     * Returns the underlying array. This method exists as performance
     * optimization to avoid extra copying of the arrays. Data inside
     * of this array should not be altered, only copied.
     *
     * @return Internal array.
     */
    public byte[] internalArray() {
        return data;
    }

    /**
     * Gets copy of internal array.
     *
     * @return Copy of internal array.
     */
    public byte[] array() {
        byte[] res = new byte[size];

        U.arrayCopy(data, 0, res, 0, size);

        return res;
    }

    /**
     * Returns internal array if it represents the whole length,
     * otherwise returns the result of {@link #array}.
     *
     * @return Array of exact data size.
     */
    public byte[] entireArray() {
        return size == data.length ? internalArray() : array();
    }

    /**
     * Gets initial capacity of the list.
     *
     * @return Initial capacity.
     */
    public int capacity() {
        return data.length;
    }

    /**
     * Sets initial capacity of the list.
     *
     * @param cap Initial capacity.
     */
    private void capacity(int cap) {
        assert cap > 0;

        if (cap != capacity()) {
            if (cap < size) {
                size = cap;

                return;
            }

            data = Arrays.copyOf(data, cap);
        }
    }

    /**
     * Gets number of bytes in the list.
     *
     * @return Number of bytes in the list.
     */
    public int size() {
        return size;
    }

    /**
     * Pre-allocates internal array for specified byte number only
     * if it currently is smaller than desired number.
     *
     * @param cnt Byte number to preallocate.
     */
    public void allocate(int cnt) {
        if (size + cnt > capacity())
            capacity(size + cnt);
    }

    /**
     * Re-sizes internal byte array representation.
     *
     * @param cnt Number of bytes to request.
     */
    private void requestFreeSize(int cnt) {
        if (size + cnt > capacity())
            capacity((size + cnt) << 1);
    }

    /**
     * Appends byte element to the list.
     *
     * @param b Byte value to append.
     */
    public void add(byte b) {
        requestFreeSize(1);

        data[size++] = b;
    }

    /**
     * Sets a byte at specified position.
     *
     * @param pos Specified position.
     * @param b Byte to set.
     */
    public void set(int pos, byte b) {
        assert pos >= 0;
        assert pos < size;

        data[pos] = b;
    }

    /**
     * Appends integer to the next 4 bytes of list.
     *
     * @param i Integer to append.
     */
    public void add(int i) {
        requestFreeSize(4);

        U.intToBytes(i, data, size);

        size += 4;
    }

    /**
     * Appends short to the next 2 bytes of the list.
     *
     * @param i Short to append.
     */
    public void add(short i) {
        requestFreeSize(2);

        U.shortToBytes(i, data, size);

        size += 2;
    }

    /**
     * Sets short at specified position.
     *
     * @param pos Specified position.
     * @param i Short to set.
     */
    public void set(int pos, short i) {
        assert pos >= 0;
        assert pos + 2 <= size;

        U.shortToBytes(i, data, pos);
    }

    /**
     * Sets integer at specified position.
     *
     * @param pos Specified position.
     * @param i Integer to set.
     */
    public void set(int pos, int i) {
        assert pos >= 0;
        assert pos + 4 <= size;

        U.intToBytes(i, data, pos);
    }

    /**
     * Appends long to the next 8 bytes of list.
     *
     * @param l Long to append.
     */
    public void add(long l) {
        requestFreeSize(8);

        U.longToBytes(l, data, size);

        size += 8;
    }

    /**
     * Sets long at specified position.
     *
     * @param pos Specified position.
     * @param l Long to set.
     */
    public void set(int pos, long l) {
        assert pos >= 0;
        assert pos + 8 <= size;

        U.longToBytes(l, data, pos);
    }

    /**
     * @param bytes Byte to add.
     * @param off Offset at which to add.
     * @param len Number of bytes to add.
     */
    public void add(byte[] bytes, int off, int len) {
        requestFreeSize(len);

        U.arrayCopy(bytes, off, data, size, len);

        size += len;
    }

    /**
     * Adds data from byte buffer into array.
     *
     * @param buf Buffer to read bytes from.
     * @param len Number of bytes to add.
     */
    public void add(ByteBuffer buf, int len) {
        requestFreeSize(len);

        buf.get(data, size, len);

        size += len;
    }

    /**
     * Gets the element (byte) at the specified position in the list.
     *
     * @param i Index of element to return.
     * @return The element at the specified position in the list.
     */
    public byte get(int i) {
        assert i < size;

        return data[i];
    }

    /**
     * Gets 4 bytes from byte list as an integer.
     *
     * @param i Index into the byte list.
     * @return Integer starting at index location.
     */
    public int getInt(int i) {
        assert i + 4 <= size;

        return U.bytesToInt(data, i);
    }

    /**
     * Reads all data from input stream until the end into this byte list.
     *
     * @param in Input stream to read from.
     * @throws IOException Thrown if any I/O error occurred.
     */
    public void readAll(InputStream in) throws IOException {
        assert in != null;

        int read = 0;

        while (read >= 0) {
            int free = capacity() - size;

            if (free == 0) {
                requestFreeSize(1);

                free = capacity() - size;

                assert free > 0;
            }

            read = in.read(data, size, free);

            if (read > 0)
                size += read;
        }
    }

    /**
     * @return Output stream based on this byte array list.
     */
    public OutputStream outputStream() {
        GridUnsafeDataOutput out = new GridUnsafeDataOutput();

        out.bytes(data, size);

        return out;
    }

    /**
     * @return Input stream based on this byte array list.
     */
    public InputStream inputStream() {
        GridUnsafeDataInput in = new GridUnsafeDataInput();

        in.bytes(data, size);

        return in;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);

        out.write(data, 0, size);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();

        data = new byte[size];

        in.readFully(data, 0, size);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray("data", data))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt("size", size))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                data = reader.readByteArray("data");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                size = reader.readInt("size");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridByteArrayList.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 84;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridByteArrayList.class, this);
    }
}