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

package org.apache.ignite.internal.util.offheap.unsafe;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.offheap.GridOffHeapEventListener;
import org.apache.ignite.internal.util.offheap.GridOffHeapOutOfMemoryException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OFFHEAP_SAFE_RELEASE;
import static org.apache.ignite.internal.util.offheap.GridOffHeapEvent.ALLOCATE;
import static org.apache.ignite.internal.util.offheap.GridOffHeapEvent.RELEASE;

/**
 * Unsafe memory.
 */
public class GridUnsafeMemory {
    /** Free byte. */
    private static final byte FREE = (byte)0;

    /** Safe offheap release flag. */
    private static final boolean SAFE_RELEASE = IgniteSystemProperties.getBoolean(IGNITE_OFFHEAP_SAFE_RELEASE);

    /** Total size. */
    @GridToStringInclude
    private final long total;

    /** Occupied size. */
    @GridToStringInclude
    private final AtomicLong allocated;

    /** Total amount of memory allocated for system structures. */
    @GridToStringInclude
    private final AtomicLong sysAllocated;

    /** Event listener. */
    private GridOffHeapEventListener lsnr;

    /**
     * @param total Total size, {@code 0} for unlimited.
     */
    public GridUnsafeMemory(long total) {
        assert total >= 0;

        this.total = total;

        allocated = new AtomicLong();

        sysAllocated = new AtomicLong();
    }

    /**
     * Sets event listener.
     *
     * @param lsnr Event listener.
     */
    public void listen(GridOffHeapEventListener lsnr) {
        this.lsnr = lsnr;
    }

    /**
     * Reserves memory.
     *
     * @param size Size to reserve.
     * @return {@code True} if memory is under allowed size, {@code false} otherwise.
     */
    public boolean reserve(long size) {
        if (total == 0) {
            allocated.addAndGet(size);

            return true;
        }

        long mem = allocated.addAndGet(size);

        long max = total;

        return mem <= max;
    }

    /**
     * Allocates memory of given size in bytes.
     *
     * @param size Size of allocated block.
     * @return Allocated block address.
     * @throws GridOffHeapOutOfMemoryException If Memory could not be allocated.
     */
    public long allocate(long size) throws GridOffHeapOutOfMemoryException {
        return allocate(size, false, false);
    }

    /**
     * Allocates memory of given size in bytes.
     *
     * @param size Size of allocated block.
     * @param init Flag to zero-out the initialized memory or not.
     * @return Allocated block address.
     * @throws GridOffHeapOutOfMemoryException If Memory could not be allocated.
     */
    public long allocate(long size, boolean init) throws GridOffHeapOutOfMemoryException {
        return allocate(size, init, false);
    }

    /**
     * Allocates memory of given size in bytes.
     *
     * @param size Size of allocated block.
     * @param init Flag to zero-out the initialized memory or not.
     * @param reserved Flag indicating that memory being allocated was reserved before.
     * @return Allocated block address.
     * @throws GridOffHeapOutOfMemoryException If memory could not be allocated.
     */
    public long allocate(long size, boolean init, boolean reserved) throws GridOffHeapOutOfMemoryException {
        return allocate0(size, init, reserved, allocated);
    }

    /**
     * Allocates memory of given size in bytes, adds to system memory counter.
     *
     * @param size Size of allocated block.
     * @param init Whether or not allocated block is zeroed upon return.
     * @return Allocated block address.
     * @throws GridOffHeapOutOfMemoryException If memory could not be allocated.
     */
    public long allocateSystem(long size, boolean init) throws GridOffHeapOutOfMemoryException {
        return allocate0(size, init, false, sysAllocated);
    }

    /**
     * Performs actual memory allocation.
     *
     * @param size Memory size to allocate.
     * @param init Flag indicating whether requested memory should be zeroed.
     * @param reserved If {@code false}, means that memory counter was reserved and size will not
     *      be added to counter.
     * @param cnt Counter to account allocated memory.
     * @throws GridOffHeapOutOfMemoryException In case of out of the off-heap memory.
     * @return Pointer to the allocated memory.
     */
    @SuppressWarnings("ErrorNotRethrown")
    private long allocate0(long size, boolean init, boolean reserved,
        AtomicLong cnt) throws GridOffHeapOutOfMemoryException {
        assert size > 0;

        if (!reserved)
            cnt.addAndGet(size);

        try {
            long ptr = GridUnsafe.allocateMemory(size);

            if (init)
                fill(ptr, size, FREE);

            if (lsnr != null)
                lsnr.onEvent(ALLOCATE);

            return ptr;
        }
        catch (OutOfMemoryError ignore) {
            if (!reserved)
                cnt.addAndGet(-size);

            throw new GridOffHeapOutOfMemoryException(totalSize(), size);
        }
    }

    /**
     * @param ptr Pointer.
     * @param size Count of long values to fill.
     * @param b Value.
     */
    public void fill(long ptr, long size, byte b) {
        GridUnsafe.setMemory(ptr, size, b);
    }

    /**
     * Releases memory at the given address.
     *
     * @param ptr Pointer to memory.
     * @param size Memory region size.
     */
    public void release(long ptr, long size) {
        release0(ptr, size, allocated);
    }

    /**
     * Releases memory allocated by {@link #allocateSystem(long, boolean)}.
     *
     * @param ptr Address of memory block to deallocate.
     * @param size Size of allocated block.
     */
    public void releaseSystem(long ptr, long size) {
        release0(ptr, size, sysAllocated);
    }

    /**
     * Internal release procedure. Decreases size of corresponding counter.
     *
     * @param ptr Address of memory block to deallocate.
     * @param size Size of allocated block.
     * @param cnt Counter to update.
     */
    private void release0(long ptr, long size, AtomicLong cnt) {
        if (ptr != 0) {
            if (SAFE_RELEASE)
                fill(ptr, size, (byte)0xAB);

            GridUnsafe.freeMemory(ptr);

            cnt.addAndGet(-size);

            if (lsnr != null)
                lsnr.onEvent(RELEASE);
        }
    }

    /**
     * @param ptr Pointer.
     * @return Long value.
     */
    public long readLong(long ptr) {
        return GridUnsafe.getLong(ptr);
    }

    /**
     * @param ptr Pointer.
     * @param v Long value.
     */
    public void writeLong(long ptr, long v) {
        GridUnsafe.putLong(ptr, v);
    }

    /**
     * @param ptr Pointer.
     * @return Long value.
     */
    public long readLongVolatile(long ptr) {
        return GridUnsafe.getLongVolatile(null, ptr);
    }

    /**
     * @param ptr Pointer.
     * @param v Long value.
     */
    public void writeLongVolatile(long ptr, long v) {
        GridUnsafe.putLongVolatile(null, ptr, v);
    }

    /**
     * @param ptr Pointer.
     * @param exp Expected.
     * @param v New value.
     * @return {@code true} If operation succeeded.
     */
    public boolean casLong(long ptr, long exp, long v) {
        return GridUnsafe.compareAndSwapLong(null, ptr, exp, v);
    }

    /**
     * @param ptr Pointer.
     * @return Integer value.
     */
    public int readInt(long ptr) {
        return GridUnsafe.getInt(ptr);
    }

    /**
     * @param ptr Pointer.
     * @param v Integer value.
     */
    public void writeInt(long ptr, int v) {
        GridUnsafe.putInt(ptr, v);
    }

    /**
     * @param ptr Pointer.
     * @return Integer value.
     */
    public int readIntVolatile(long ptr) {
        return GridUnsafe.getIntVolatile(null, ptr);
    }

    /**
     * @param ptr Pointer.
     * @param v Integer value.
     */
    public void writeIntVolatile(long ptr, int v) {
        GridUnsafe.putIntVolatile(null, ptr, v);
    }

    /**
     * @param ptr Pointer.
     * @param exp Expected.
     * @param v New value.
     * @return {@code true} If operation succeeded.
     */
    public boolean casInt(long ptr, int exp, int v) {
        return GridUnsafe.compareAndSwapInt(null, ptr, exp, v);
    }

    /**
     * @param ptr Pointer.
     * @return Float value.
     */
    public float readFloat(long ptr) {
        return GridUnsafe.getFloat(ptr);
    }

    /**
     * @param ptr Pointer.
     * @param v Value.
     */
    public void writeFloat(long ptr, float v) {
        GridUnsafe.putFloat(ptr, v);
    }

    /**
     * @param ptr Pointer.
     * @return Double value.
     */
    public double readDouble(long ptr) {
        return GridUnsafe.getDouble(ptr);
    }

    /**
     * @param ptr Pointer.
     * @param v Value.
     */
    public void writeDouble(long ptr, double v) {
        GridUnsafe.putDouble(ptr, v);
    }

    /**
     * @param ptr Pointer.
     * @return Short value.
     */
    public short readShort(long ptr) {
        return GridUnsafe.getShort(ptr);
    }

    /**
     * @param ptr Pointer.
     * @param v Short value.
     */
    public void writeShort(long ptr, short v) {
        GridUnsafe.putShort(ptr, v);
    }

    /**
     * @param ptr Pointer.
     * @return Integer value.
     */
    public byte readByte(long ptr) {
        return GridUnsafe.getByte(ptr);
    }

    /**
     * @param ptr Pointer.
     * @return Integer value.
     */
    public byte readByteVolatile(long ptr) {
        return GridUnsafe.getByteVolatile(null, ptr);
    }

    /**
     * @param ptr Pointer.
     * @param v Integer value.
     */
    public void writeByte(long ptr, byte v) {
        GridUnsafe.putByte(ptr, v);
    }

    /**
     * @param ptr Pointer.
     * @param v Integer value.
     */
    public void writeByteVolatile(long ptr, byte v) {
        GridUnsafe.putByteVolatile(null, ptr, v);
    }

    /**
     * Stores value to the specified memory location. If specified pointer is {@code 0}, then will
     * allocate required space. If size of allocated space is not enough to hold given values, will
     * reallocate memory.
     *
     * @param ptr Optional pointer to allocated memory. First 4 bytes in allocated region must contain
     *      size of allocated chunk.
     * @param val Value to store.
     * @param type Value type.
     * @return Pointer.
     */
    public long putOffHeap(long ptr, byte[] val, byte type) {
        int size = val.length;

        assert size != 0;

        int allocated = ptr == 0 ? 0 : readInt(ptr);

        if (allocated != size) {
            if (ptr != 0)
                release(ptr, allocated + 5);

            ptr = allocate(size + 5);

            writeInt(ptr, size);
        }

        writeByte(ptr + 4, type);
        writeBytes(ptr + 5, val);

        return ptr;
    }

    /**
     * Releases off-heap memory allocated by {@link #putOffHeap} method.
     *
     * @param ptr Optional pointer returned by {@link #putOffHeap}.
     */
    public void removeOffHeap(long ptr) {
        if (ptr != 0)
            release(ptr, readInt(ptr) + 5);
    }

    /**
     * Get value stored in offheap along with a value type.
     *
     * @param ptr Pointer to read.
     * @return Stored byte array and "raw bytes" flag.
     */
    public IgniteBiTuple<byte[], Byte> get(long ptr) {
        assert ptr != 0;

        int size = readInt(ptr);

        byte type = readByte(ptr + 4);
        byte[] bytes = readBytes(ptr + 5, size);

        return new IgniteBiTuple<>(bytes, type);
    }

    /**
     * @param ptr1 First pointer.
     * @param ptr2 Second pointer.
     * @param size Memory size.
     * @return {@code True} if equals.
     */
    public static boolean compare(long ptr1, long ptr2, int size) {
        assert ptr1 > 0 : ptr1;
        assert ptr2 > 0 : ptr2;
        assert size > 0 : size;

        if (ptr1 == ptr2)
            return true;

        int words = size / 8;

        for (int i = 0; i < words; i++) {
            long w1 = GridUnsafe.getLong(ptr1);
            long w2 = GridUnsafe.getLong(ptr2);

            if (w1 != w2)
                return false;

            ptr1 += 8;
            ptr2 += 8;
        }

        int left = size % 8;

        for (int i = 0; i < left; i++) {
            byte b1 = GridUnsafe.getByte(ptr1);
            byte b2 = GridUnsafe.getByte(ptr2);

            if (b1 != b2)
                return false;

            ptr1++;
            ptr2++;
        }

        return true;
    }

    /**
     * Compares memory.
     *
     * @param ptr Pointer.
     * @param bytes Bytes to compare.
     * @return {@code True} if equals.
     */
    public static boolean compare(long ptr, byte[] bytes) {
        return compare(ptr, bytes, 0, bytes.length);
    }

    /**
     * Compares memory.
     *
     * @param ptr Pointer.
     * @param bytes Bytes to compare.
     * @param bytesOff Offset in the bytes array.
     * @param len Count of compared bytes.
     * @return {@code True} if equals.
     */
    public static boolean compare(long ptr, byte[] bytes, int bytesOff, int len) {
        assert bytesOff + len <= bytes.length : "Check compare bounds: [offset=" + bytesOff + ", len=" + len +
            ", bytes.length=" + bytes.length + ']';

        final int addrSize = GridUnsafe.ADDR_SIZE;

        // Align reads to address size.
        int off = (int)(ptr % addrSize);
        int align = addrSize - off;

        if (align != addrSize) {
            for (int i = 0, tmpOff = bytesOff; i < align && i < len; i++, tmpOff++, ptr++) {
                if (GridUnsafe.getByte(ptr) != bytes[tmpOff])
                    return false;
            }
        }
        else
            align = 0;

        if (len <= align)
            return true;

        assert ptr % addrSize == 0 : "Invalid alignment [ptr=" + ptr + ", addrSize=" + addrSize + ", mod=" +
            (ptr % addrSize) + ']';

        int words = (len - align) / addrSize;
        int left = (len - align) % addrSize;

        switch (addrSize) {
            case 4:
                for (int i = 0; i < words; i++) {
                    int step = i * addrSize + align;

                    int word = GridUnsafe.getInt(ptr);

                    int comp = GridUnsafe.getInt(bytes, GridUnsafe.BYTE_ARR_OFF + step + bytesOff);

                    if (word != comp)
                        return false;

                    ptr += GridUnsafe.ADDR_SIZE;
                }

                break;

            default:
                for (int i = 0; i < words; i++) {
                    int step = i * addrSize + align;

                    long word = GridUnsafe.getLong(ptr);

                    long comp = GridUnsafe.getLong(bytes, GridUnsafe.BYTE_ARR_OFF + step + bytesOff);

                    if (word != comp)
                        return false;

                    ptr += GridUnsafe.ADDR_SIZE;
                }

                break;
        }

        if (left != 0) {
            // Compare left overs byte by byte.
            for (int i = 0; i < left; i++)
                if (GridUnsafe.getByte(ptr + i) != bytes[bytesOff + i + align + words * GridUnsafe.ADDR_SIZE])
                    return false;
        }

        return true;
    }

    /**
     * @param ptr Pointer.
     * @param cnt Count.
     * @return Byte array.
     */
    public byte[] readBytes(long ptr, int cnt) {
        return readBytes(ptr, new byte[cnt]);
    }

    /**
     * @param ptr Pointer.
     * @param arr Array.
     * @return The same array as passed in one.
     */
    public byte[] readBytes(long ptr, byte[] arr) {
        GridUnsafe.copyOffheapHeap(ptr, arr, GridUnsafe.BYTE_ARR_OFF, arr.length);

        return arr;
    }

    /**
     * @param ptr Pointer.
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     * @return The same array as passed in one.
     */
    public byte[] readBytes(long ptr, byte[] arr, int off, int len) {
        GridUnsafe.copyOffheapHeap(ptr, arr, GridUnsafe.BYTE_ARR_OFF + off, len);

        return arr;
    }

    /**
     * Writes byte array into memory location.
     *
     * @param ptr Pointer.
     * @param arr Array.
     */
    public void writeBytes(long ptr, byte[] arr) {
        GridUnsafe.copyHeapOffheap(arr, GridUnsafe.BYTE_ARR_OFF, ptr, arr.length);
    }

    /**
     * Writes part of byte array into memory location.
     *
     * @param ptr Pointer.
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     */
    public void writeBytes(long ptr, byte[] arr, int off, int len) {
        GridUnsafe.copyHeapOffheap(arr, GridUnsafe.BYTE_ARR_OFF + off, ptr, len);
    }

    /**
     * Copy memory.
     *
     * @param srcPtr Source pointer.
     * @param destPtr Destination pointer.
     * @param len Length in bytes.
     */
    public void copyMemory(long srcPtr, long destPtr, long len) {
        GridUnsafe.copyOffheapOffheap(srcPtr, destPtr, len);
    }

    /**
     * Checks if direct memory allocation is limited to some value.
     *
     * @return {@code True} if memory allocation is limited.
     */
    public boolean unlimited() {
        return totalSize() == 0;
    }

    /**
     * @return Total size.
     */
    public long totalSize() {
        return total;
    }

    /**
     * @return Free size.
     */
    public long freeSize() {
        if (total == 0)
            return 0;

        long diff = total - allocated.get();

        return diff < 0 ? 0 : diff;
    }

    /**
     * @return Allocated size.
     */
    public long allocatedSize() {
        return allocated.get();
    }

    /**
     * @return Size of memory allocated with {@link #allocateSystem(long, boolean)}.
     */
    public long systemAllocatedSize() {
        return sysAllocated.get();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUnsafeMemory.class, this);
    }
}
