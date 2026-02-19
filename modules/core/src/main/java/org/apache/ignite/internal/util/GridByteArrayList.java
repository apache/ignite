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
import java.util.Arrays;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Re-sizable array implementation of the byte list (eliminating auto-boxing of primitive byte type).
 */
public class GridByteArrayList implements Message, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** List byte data. */
    @Order(value = 0, method = "internalArray")
    @GridToStringExclude
    private byte[] data;

    /** List's size. */
    @Order(1)
    int size;

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
     * @param data The underlying array.
     */
    public void internalArray(byte[] data) {
        this.data = data;
    }

    /**
     * Gets copy of internal array.
     *
     * @return Copy of internal array.
     */
    public byte[] array() {
        byte[] res = new byte[size];

        GridUnsafe.arrayCopy(data, 0, res, 0, size);

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
     * Sets initial capacity of the list.
     *
     * @param cap Initial capacity.
     */
    private void capacity(int cap) {
        assert cap > 0;

        if (cap != data.length) {
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
     * @param size Number of bytes in the list.
     */
    public void size(int size) {
        this.size = size;
    }

    /**
     * Re-sizes internal byte array representation.
     *
     * @param cnt Number of bytes to request.
     */
    private void requestFreeSize(int cnt) {
        if (size + cnt > data.length)
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
     * @param bytes Byte to add.
     * @param off Offset at which to add.
     * @param len Number of bytes to add.
     */
    public void add(byte[] bytes, int off, int len) {
        requestFreeSize(len);

        GridUnsafe.arrayCopy(bytes, off, data, size, len);

        size += len;
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
            int free = data.length - size;

            if (free == 0) {
                requestFreeSize(1);

                free = data.length - size;

                assert free > 0;
            }

            read = in.read(data, size, free);

            if (read > 0)
                size += read;
        }
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
    @Override public short directType() {
        return 84;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridByteArrayList.class, this);
    }
}
