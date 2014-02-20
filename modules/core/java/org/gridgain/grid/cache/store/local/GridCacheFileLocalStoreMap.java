// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store.local;

import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.nio.*;

/**
 * Not thread safe offheap data structure to store mappings from {@code int} key to list of 48-bit {@code long} values.
 * Does not support {@code 0} key because it is reserved as marker for empty slot in map.
 *
 * @author @java.author
 * @version @java.version
 */
class GridCacheFileLocalStoreMap implements AutoCloseable {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    /** 4 bytes key + 6 bytes value. */
    private static final long ENTRY_SIZE = 10;

    /** 2 bytes shift for long value. */
    private static final int VAL_SHIFT = 16;

    /** 6 bytes mask for long value. */
    static final long VAL_MASK = (~0L) >>> VAL_SHIFT;

    /** Number of sequential entries to scan. */
    private static final int SCAN = 3;

    /** Offheap pointer to memory block for this table. */
    private long data;

    /** Number of entries in table. */
    private int size;

    /** Capacity in entries (always power of 2). */
    private int cap;

    /** */
    private GridCacheFileLocalStoreMap next;

    /**
     * @param cap Capacity. Must be power of 2.
     */
    GridCacheFileLocalStoreMap(int cap) {
        reset(cap);
    }

    /**
     * @param cap Capacity.
     */
    void reset(int cap) {
        assert U.isPow2(cap) : cap;

        long bytesSize = cap * ENTRY_SIZE;

        data = UNSAFE.allocateMemory(bytesSize);

        UNSAFE.setMemory(data, bytesSize, (byte)0);

        this.cap = cap;
    }

    /**
     */
    void rehash() {
        assert data != 0;

        long oldData = data;
        int oldCap = cap;
        int oldMask = oldCap - 1;

        int cap = this.cap << 1;
        long offDelta = oldCap * ENTRY_SIZE;

        reset(cap);

        int mask = cap - 1;

        int s = size;

        for (int i = 0; i < oldCap; i++) {
            long off = offset(i, 0, oldCap);

            int key = key(oldData, off);

            if (key != 0) {
                long v = value(oldData, off);

                assert key(data, off + offDelta) == 0;

                int oldPos = (key & oldMask);

                long newOff = off;

                // Key with new mask changes position and no overflow or does not change but overflow occurred.
                if ((key & mask) != oldPos ^ oldPos > i)
                    newOff += offDelta;

                set(newOff, key, v);

                if (next != null) { // Try to promote values from upper tables.
                    boolean moved;

                    if (newOff != off)
                        moved = next.moveCollision(this, off, i);
                    else
                        moved = next.moveCollision(this, off + offDelta, i + oldCap);

                    if (moved) {
                        size++;

                        if (next.size == 0)
                            next = next.next;
                    }
                }

                if (--s == 0) // Found all the entries.
                    break;
            }
        }

        UNSAFE.freeMemory(oldData);
    }

    /**
     * @param key Key.
     * @param scan Scan index.
     * @param cap Capacity.
     * @return Offset.
     */
    static long offset(int key, int scan, int cap) {
        return ((key + scan) & (cap - 1L)) * ENTRY_SIZE;
    }

    /**
     * @param data Data.
     * @param off Offset.
     * @return Key.
     */
    static int key(long data, long off) {
        assert data != 0;

        return UNSAFE.getInt(data + off);
    }

    /**
     * @param data Data.
     * @param off Offset.
     * @param key Key.
     */
    static void key(long data, long off, int key) {
        assert data != 0;

        UNSAFE.putInt(data + off, key);
    }

    /**
     * @param data Data.
     * @param off Offset.
     * @return Value.
     */
    static long value(long data, long off) {
        assert data != 0;

        long v = UNSAFE.getLong(data + off + 2);

        return LITTLE_ENDIAN ? v >>> VAL_SHIFT : v & VAL_MASK;
    }

    /**
     * !!WARNING!! Value must be always written before key since it will override part of key otherwise.
     *
     * @param data Data array.
     * @param off Offset.
     * @param v Value.
     */
    static void value(long data, long off, long v) {
        assert data != 0;
        assert (v & VAL_MASK) == v;

        if (LITTLE_ENDIAN)
            v <<= VAL_SHIFT;

        UNSAFE.putLong(data + off + 2, v);
    }

    /**
     * @param key Key.
     * @param val Value.
     */
    public void add(int key, long val) {
        assert key != 0;

        if (doAdd(key, val) != -1) {
            size++;

            if (cap - size < cap >>> 2) // Less than 1/4 free.
                rehash();
        }
        else {
            if (next == null)
                next = new GridCacheFileLocalStoreMap(32);

            next.add(key, val);
        }
    }

    /**
     * @param key Key.
     * @param val Value.
     * @return Offset of entry in array if succeeded, {@code -1} otherwise.
     */
    private long doAdd(int key, long val) {
        for (int i = 0; i < SCAN; i++) {
            long off = offset(key, i, cap);

            if (key(data, off) == 0) { // Found empty cell.
                set(off, key, val);

                return off;
            }
        }

        return -1;
    }

    /**
     * @param key Key.
     * @param oldVal Old value.
     * @param newVal New value.
     * @return {@code true} If succeeded.
     */
    public boolean replace(int key, long oldVal, long newVal) {
        assert key != 0;

        for (int i = 0; i < SCAN; i++) {
            long off = offset(key, i, cap);

            if (key(data, off) == key && value(data, off) == oldVal) {
                set(off, key, newVal);

                return true;
            }
        }

        return next != null && next.replace(key, oldVal, newVal);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @return {@code true} If succeeded.
     */
    public boolean remove(int key, long val) {
        assert key != 0;

        for (int i = 0; i < SCAN; i++) {
            long off = offset(key, i, cap);

            if (key(data, off) == key && value(data, off) == val) {
                doRemove(off, (key & (cap - 1)) + i);

                return true;
            }
        }

        if (next != null && next.remove(key, val)) {
            if (next.size == 0)
                next = next.next; // Remove empty table.

            return true;
        }

        return false;
    }

    /**
     * @param off Offset.
     * @param key Key.
     * @param val Value.
     */
    void set(long off, int key, long val) {
        set(data, off, key, val);
    }

    /**
     * @param data Data.
     * @param off Offset.
     * @param key Key.
     * @param val Value.
     */
    static void set(long data, long off, int key, long val) {
        // Updates in right order (value can partially override key, so it goes first).
        value(data, off, val);
        key(data, off, key);
    }

    /**
     * @param off Key offset.
     * @param pos Key position.
     */
    private void doRemove(long off, int pos) {
        // Try to move collisions from next tables.
        if (next != null && next.moveCollision(this, off, pos)) {
            if (next.size == 0)
                next = next.next; // Remove empty table.

            return;
        }

        // Failed to move collisions, resetting key to 0.
        key(data, off, 0);

        size--;
    }

    /**
     * @param to Target table.
     * @param toOff Target table offset.
     * @param pos Position in target table.
     * @return {@code true} If key was moved.
     */
    private boolean moveCollision(GridCacheFileLocalStoreMap to, final long toOff, final int pos) {
        // Start from top to bottom to remove smallest tables first.
        if (next != null && next.moveCollision(to, toOff, pos)) {
            if (next.size == 0)
                next = next.next; // Remove empty table.

            return true;
        }

        int maskTo = to.cap - 1;

        // Position in this table.
        int pos0 = pos & (cap - 1);

        // Find first close enough key in surroundings of pos0.
        for (int i = 1 - SCAN; i < SCAN; i++) {
            long off = offset(pos0, i, cap);

            int key = key(data, off);

            if (key == 0) // No entry here.
                continue;

            int maskedKey = key & maskTo;
            int gap = pos - maskedKey;

            if (gap >= 0 && gap < SCAN) {
                to.set(toOff, key, value(data, off));

                doRemove(off, pos0 + i);

                return true;
            }
        }

        return false;
    }

    /**
     * @param key Key.
     * @param res List to collect result.
     * @return List of results (given or created one).
     */
    public GridLongList get(int key, @Nullable GridLongList res) {
        assert key != 0;

        for (int i = 0; i < SCAN; i++) {
            long off = offset(key, i, cap);

            if (key(data, off) == key) {
                if (res == null)
                    res = new GridLongList(2);

                res.add(value(data, off));
            }
        }

        if (next != null)
            return next.get(key, res);

        return res;
    }

    /**
     * @param key Key.
     * @param val Value.
     * @return {@code true} If contains.
     */
    public boolean contains(int key, long val) {
        assert key != 0;

        for (int i = 0; i < SCAN; i++) {
            long off = offset(key, i, cap);

            if (key(data, off) == key && value(data, off) == val)
                return true;
        }

        return next != null && next.contains(key, val);
    }

    /**
     * @param withChildren Take size of children as well.
     * @return Size.
     */
    public int size(boolean withChildren) {
        int res = 0;

        GridCacheFileLocalStoreMap t = this;

        do {
            res += t.size;

            t = t.next;
        }
        while(withChildren && t != null);

        return res;
    }

    /**
     * @param c Closure.
     * @throws GridException If failed.
     */
    public void iterate(Closure c) throws GridException {
        for (int i = 0; i < cap; i++) {
            long off = offset(i, 0, cap);

            int key = key(data, off);

            if (key != 0) {
                long val = value(data, off);

                c.apply(val);
            }
        }

        if (next != null)
            next.iterate(c);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (next != null)
            next.close();

        if (data != 0) {
            UNSAFE.freeMemory(data);

            data = 0;
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static interface Closure {
        /**
         * @param val Value.
         * @throws GridException If failed.
         */
        public void apply(long val) throws GridException;
    }
}