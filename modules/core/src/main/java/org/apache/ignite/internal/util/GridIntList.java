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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Minimal list API to work with primitive ints. This list exists
 * to avoid boxing/unboxing when using standard list from Java.
 */
public class GridIntList implements Message, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int[] arr;

    /** */
    private int idx;

    /**
     *
     */
    public GridIntList() {
        // No-op.
    }

    /**
     * @param size Size.
     */
    public GridIntList(int size) {
        arr = new int[size];
        // idx = 0
    }

    /**
     * @param arr Array.
     */
    public GridIntList(int[] arr) {
        this.arr = arr;

        idx = arr.length;
    }

    /**
     * @param vals Values.
     * @return List from values.
     */
    public static GridIntList asList(int... vals) {
        if (F.isEmpty(vals))
            return new GridIntList();

        return new GridIntList(vals);
    }

    /**
     * @param arr Array.
     * @param size Size.
     */
    private GridIntList(int[] arr, int size) {
        this.arr = arr;
        idx = size;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridIntList))
            return false;

        GridIntList that = (GridIntList)o;

        if (idx != that.idx)
            return false;

        if (idx == 0 || arr == that.arr)
            return true;

        for (int i = 0; i < idx; i++) {
            if (arr[i] != that.arr[i])
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        for (int i = 0; i < idx; i++) {
            int element = arr[i];
            res = 31 * res + element;
        }

        return res;
    }

    /**
     * Add element to this array.
     * @param x Value.
     */
    public void add(int x) {
        if (arr == null)
            arr = new int[4];
        else if (arr.length == idx)
            arr = Arrays.copyOf(arr, arr.length << 1);

        arr[idx++] = x;
    }

    /**
     * Returns (possibly reordered) copy of this list, excluding all elements of given list.
     *
     * @param l List of elements to remove.
     * @return New list without all elements from {@code l}.
     */
    public GridIntList copyWithout(GridIntList l) {
        assert l != null;

        if (idx == 0)
            return new GridIntList();

        if (l.idx == 0)
            return new GridIntList(Arrays.copyOf(arr, idx));

        int[] newArr = Arrays.copyOf(arr, idx);
        int newIdx = idx;

        for (int i = 0; i < l.size(); i++) {
            int rmVal = l.get(i);

            for (int j = 0; j < newIdx; j++) {
                if (newArr[j] == rmVal) {

                    while (newIdx > 0 && newArr[newIdx - 1] == rmVal)
                        newIdx--;

                    if (newIdx > 0) {
                        newArr[j] = newArr[newIdx - 1];
                        newIdx--;
                    }
                }
            }
        }

        return new GridIntList(newArr, newIdx);
    }

    /**
     * @param i Index.
     * @return Value.
     */
    public int get(int i) {
        assert i < idx;

        return arr[i];
    }

    /**
     * @return Size.
     */
    public int size() {
        return idx;
    }

    /**
     * @return {@code True} if this list has no elements.
     */
    public boolean isEmpty() {
        return idx == 0;
    }

    /**
     * @param l Element to find.
     * @return {@code True} if found.
     */
    public boolean contains(int l) {
        for (int i = 0; i < idx; i++) {
            if (arr[i] == l)
                return true;
        }

        return false;
    }

    /**
     * @param size New size.
     * @param last If {@code true} the last elements will be removed, otherwise the first.
     */
    public void truncate(int size, boolean last) {
        assert size >= 0 && size <= idx;

        if (size == idx)
            return;

        if (!last && size != 0)
            System.arraycopy(arr, idx - size, arr, 0, size);

        idx = size;
    }

    /**
     * Removes element by given index.
     *
     * @param i Index.
     * @return Removed value.
     */
    public int removeIndex(int i) {
        assert i < idx : i;

        int res = arr[i];

        if (i == idx - 1) { // Last element.
            idx = i;
        }
        else {
            System.arraycopy(arr, i + 1, arr, i, idx - i - 1);
            idx--;
        }

        return res;
    }

    /**
     * Removes value from this list.
     *
     * @param startIdx Index to begin search with.
     * @param val Value.
     * @return Index of removed value if the value was found and removed or {@code -1} otherwise.
     */
    public int removeValue(int startIdx, int val) {
        assert startIdx >= 0;

        for (int i = startIdx; i < idx; i++) {
            if (arr[i] == val) {
                removeIndex(i);

                return i;
            }
        }

        return -1;
    }

    /**
     * @return Array copy.
     */
    public int[] array() {
        int[] res = new int[idx];

        System.arraycopy(arr, 0, res, 0, idx);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(idx);

        for (int i = 0; i < idx; i++)
            out.writeInt(arr[i]);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        idx = in.readInt();

        arr = new int[idx];

        for (int i = 0; i < idx; i++)
            arr[i] = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB b = new SB("[");

        for (int i = 0; i < idx; i++) {
            if (i != 0)
                b.a(',');

            b.a(arr[i]);
        }

        b.a(']');

        return b.toString();
    }

    /**
     * Sorts this list.
     * Use {@code copy().sort()} if you need a defensive copy.
     *
     * @return {@code this} For chaining.
     */
    public GridIntList sort() {
        if (idx > 1)
            Arrays.sort(arr, 0, idx);

        return this;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeIntArray("arr", arr))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt("idx", idx))
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
                arr = reader.readIntArray("arr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                idx = reader.readInt("idx");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridIntList.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -52;
    }

    /**
     * @return Iterator.
     */
    public GridIntIterator iterator() {
        return new GridIntIterator() {
            int c;

            @Override public boolean hasNext() {
                return c < idx;
            }

            @Override public int next() {
                return arr[c++];
            }
        };
    }
}
