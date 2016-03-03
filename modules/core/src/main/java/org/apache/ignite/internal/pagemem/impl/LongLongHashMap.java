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

package org.apache.ignite.internal.pagemem.impl;


import org.apache.ignite.internal.pagemem.DirectMemoryUtils;
import org.apache.ignite.internal.mem.OutOfMemoryException;
import org.apache.ignite.internal.util.typedef.internal.A;

import static org.apache.ignite.IgniteSystemProperties.*;

public class LongLongHashMap {
    /** Load factor. */
    private static final int LOAD_FACTOR = getInteger(IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR, 2);

    /** */
    public static final long EMPTY = 0;

    /** */
    public static final long REMOVED = 0x8000000000000000L;

    /** Max size, in elements. */
    protected int capacity;

    /** Maximum number of steps to try before failing. */
    protected int maxSteps;

    /** Pointer to the values array. */
    protected long valPtr;

    /** */
    protected DirectMemoryUtils mem;

    /**
     * @return Estimated memory size required for this map to store the given number of elements.
     */
    public static long requiredMemory(long elementCnt) {
        assert LOAD_FACTOR != 0;

        return elementCnt * 16 * LOAD_FACTOR + 4;
    }

    /**
     * @param mem Memory interface.
     * @param addr Base address.
     * @param len Allocated memory length.
     * @param clear If {@code true}, then memory is considered dirty and will be cleared. Otherwise,
     *      map will assume that the given memory region is in valid state.
     */
    public LongLongHashMap(DirectMemoryUtils mem, long addr, long len, boolean clear) {
        valPtr = addr;
        capacity = (int)((len - 4) / 16);
        maxSteps = (int)Math.sqrt(capacity);
        this.mem = mem;

        if (clear)
            clear();
    }

    /**
     * @return Current number of entries in the map.
     */
    public final int size() {
        return mem.readInt(valPtr);
    }

    /**
     * @return Maximum number of entries in the map. This maximum can not be always reached.
     */
    public final int capacity() {
        return capacity;
    }

    /**
     * Gets value associated with the given key.
     *
     * @param key Key to get value for. Key cannot be equal to {@code 0} and key cannot be equal
     * to {@code 0x8000000000000000}.
     *
     * @return A value associated with the given key.
     */
    public long get(long key, long absent) {
        A.ensure(key != 0, "key != 0");
        A.ensure(key != REMOVED, "key != 0x8000000000000000");

        int index = getKey(key);

        if (index < 0)
            return absent;

        return valueAt(index);
    }

    /**
     * Associates the given key with the given value.
     *
     * @param key Key to set value for. Key cannot be equal to {@code 0} and key cannot be equal
     *      to {@code 0x8000000000000000}.
     * @param value Value to set.
     */
    public void put(long key, long value) {
        A.ensure(key != 0, "key != 0");
        A.ensure(key != REMOVED, "key != 0x8000000000000000");

        int index = putKey(key);

        setValueAt(index, value);
    }

    /**
     * Associates the given key with the given value only if currently there is no value associated with the key.
     *
     * @param key Key to set value for. Key cannot be equal to {@code 0} and key cannot be equal
     *      to {@code 0x8000000000000000}.
     * @param value Value to set.
     * @return {@code 0} if there was no old value and mapping was set, old value otherwise.
     */
    public long putIfAbsent(long key, long value) {
        A.ensure(key != 0, "key != 0");
        A.ensure(key != REMOVED, "key != 0x8000000000000000");

        int step = 1;
        int index = hash(key) % capacity;

        do {
            long cur = keyAt(index);

            if (cur == EMPTY || cur == REMOVED) {
                setKeyAt(index, key);

                setValueAt(index, value);

                incrementSize();

                return 0;
            }
            else if (cur == key)
                return valueAt(index);

            if ((index += step) >= capacity)
                index -= capacity;
        } while (++step <= maxSteps);

        throw new OutOfMemoryException("No room for a new key");
    }

    /**
     * Changes the value currently associated with the given key.
     *
     * @param key Key to change mapping for. Key cannot be equal to {@code 0} and key cannot be equal
     *      to {@code 0x8000000000000000}.
     * @param oldValue Old value.
     * @param newValue New value.
     * @return Old value.
     */
    public boolean replace(long key, long oldValue, long newValue) {
        A.ensure(key != 0, "key != 0");
        A.ensure(key != REMOVED, "key != 0x8000000000000000");

        int index = getKey(key);

        if (index < 0)
            return false;

        long old = valueAt(index);

        if (old == oldValue) {
            setValueAt(index, newValue);

            return true;
        }

        return false;
    }

    /**
     * Equivalent to {@link #put(long, long)}, but returns the previous value associated with the key.
     *
     * @param key Key to update. Key cannot be equal to {@code 0} and key cannot be equal
     *      to {@code 0x8000000000000000}.
     * @param newValue New value to set.
     * @return Old value.
     */
    public long replace(long key, long newValue) {
        A.ensure(key != 0, "key != 0");
        A.ensure(key != REMOVED, "key != 0x8000000000000000");

        int index = putKey(key);

        assert index >= 0;

        long old = valueAt(index);

        setValueAt(index, newValue);

        return old;
    }

    /**
     * Removes key-value association for the given key.
     *
     * @param key Key to remove from the map.
     */
    public void remove(long key) {
        A.ensure(key != 0, "key != 0");
        A.ensure(key != REMOVED, "key != 0x8000000000000000");

        int index = removeKey(key);

        if (index >= 0)
            setValueAt(index, 0);
    }

    /**
     * @param key Key.
     * @return Key index.
     */
    private int putKey(long key) {
        int step = 1;

        int index = hash(key) % capacity;

        do {
            long cur = keyAt(index);

            if (cur == EMPTY) {
                setKeyAt(index, key);

                incrementSize();

                return index;
            }
            else if (cur == key)
                return index;

            if ((index += step) >= capacity)
                index -= capacity;
        }
        while (++step <= maxSteps);

        throw new OutOfMemoryException("No room for a new key");
    }

    /**
     * @param key Key.
     * @return Key index.
     */
    private int getKey(long key) {
        int step = 1;

        int index = hash(key) % capacity;

        do {
            long cur = keyAt(index);

            if (cur == key)
                return index;
            else if (cur == EMPTY)
                return -1;

            if ((index += step) >= capacity)
                index -= capacity;
        } while (++step <= maxSteps);

        return -1;
    }

    /**
     * @param key Key.
     * @return Key index.
     */
    private int removeKey(long key) {
        int step = 1;

        int index = hash(key) % capacity;

        do {
            long cur = keyAt(index);

            if (cur == key) {
                setKeyAt(index, REMOVED);

                decrementSize();

                return index;
            }
            else if (cur == EMPTY)
                return -1;

            if ((index += step) >= capacity)
                index -= capacity;
        }
        while (++step <= maxSteps);

        return -1;
    }

    /**
     * @param index Entry index.
     * @return Key value.
     */
    private long keyAt(int index) {
        return mem.readLong(valPtr + 4 + (long)index * 16);
    }

    /**
     * @param index Entry index.
     * @param value Key value.
     */
    private void setKeyAt(int index, long value) {
        mem.writeLong(valPtr + 4 + (long)index * 16, value);
    }

    /**
     * @param index Entry index.
     * @return Value.
     */
    private long valueAt(int index) {
        return mem.readLong(valPtr + 4 + (long)index * 16 + 8);
    }

    /**
     * @param index Entry index.
     * @param value Value.
     */
    private void setValueAt(int index, long value) {
        mem.writeLong(valPtr + 4 + (long)index * 16 + 8, value);
    }

    /**
     *
     */
    public void clear() {
        mem.setMemory(valPtr, capacity * 16 + 4, (byte)EMPTY);
    }

    /**
     *
     */
    private void incrementSize() {
        mem.writeInt(valPtr, mem.readInt(valPtr) + 1);
    }

    /**
     *
     */
    private void decrementSize() {
        mem.writeInt(valPtr, mem.readInt(valPtr) + 1);
    }

    /**
     * @param key Key to get hash for.
     * @return Key hash.
     */
    protected static int hash(long key) {
        return ((int)key ^ (int)(key >>> 21) ^ (int)(key >>> 42)) & 0x7fffffff;
    }
}
