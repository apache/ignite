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

package org.apache.ignite.internal.processors.platform.memory;

import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Utility classes for memory management.
 */
public class PlatformMemoryUtils {
    /** Header length. */
    public static final int POOL_HDR_LEN = 64;

    /** Pool header offset: first memory chunk. */
    public static final int POOL_HDR_OFF_MEM_1 = 0;

    /** Pool header offset: second memory chunk. */
    public static final int POOL_HDR_OFF_MEM_2 = 20;

    /** Pool header offset: third memory chunk. */
    public static final int POOL_HDR_OFF_MEM_3 = 40;

    /** Memory chunk header length. */
    public static final int MEM_HDR_LEN = 20;

    /** Offset: capacity. */
    public static final int MEM_HDR_OFF_CAP = 8;

    /** Offset: length. */
    public static final int MEM_HDR_OFF_LEN = 12;

    /** Offset: flags. */
    public static final int MEM_HDR_OFF_FLAGS = 16;

    /** Flag: external. */
    public static final int FLAG_EXT = 0x1;

    /** Flag: pooled. */
    public static final int FLAG_POOLED = 0x2;

    /** Flag: whether this pooled memory chunk is acquired. */
    public static final int FLAG_ACQUIRED = 0x4;

    /** --- COMMON METHODS. --- */

    /**
     * Gets data pointer for the given memory chunk.
     *
     * @param memPtr Memory pointer.
     * @return Data pointer.
     */
    public static long data(long memPtr) {
        return GridUnsafe.getLong(memPtr);
    }

    /**
     * Gets capacity for the given memory chunk.
     *
     * @param memPtr Memory pointer.
     * @return Capacity.
     */
    public static int capacity(long memPtr) {
        return GridUnsafe.getInt(memPtr + MEM_HDR_OFF_CAP);
    }

    /**
     * Sets capacity for the given memory chunk.
     *
     * @param memPtr Memory pointer.
     * @param cap Capacity.
     */
    public static void capacity(long memPtr, int cap) {
        assert !isExternal(memPtr) : "Attempt to update external memory chunk capacity: " + memPtr;

        GridUnsafe.putInt(memPtr + MEM_HDR_OFF_CAP, cap);
    }

    /**
     * Gets length for the given memory chunk.
     *
     * @param memPtr Memory pointer.
     * @return Length.
     */
    public static int length(long memPtr) {
        return GridUnsafe.getInt(memPtr + MEM_HDR_OFF_LEN);
    }

    /**
     * Sets length for the given memory chunk.
     *
     * @param memPtr Memory pointer.
     * @param len Length.
     */
    public static void length(long memPtr, int len) {
        GridUnsafe.putInt(memPtr + MEM_HDR_OFF_LEN, len);
    }

    /**
     * Gets flags for the given memory chunk.
     *
     * @param memPtr Memory pointer.
     * @return Flags.
     */
    public static int flags(long memPtr) {
        return GridUnsafe.getInt(memPtr + MEM_HDR_OFF_FLAGS);
    }

    /**
     * Sets flags for the given memory chunk.
     *
     * @param memPtr Memory pointer.
     * @param flags Flags.
     */
    public static void flags(long memPtr, int flags) {
        assert !isExternal(memPtr) : "Attempt to update external memory chunk flags: " + memPtr;

        GridUnsafe.putInt(memPtr + MEM_HDR_OFF_FLAGS, flags);
    }

    /**
     * Check whether this memory chunk is external.
     *
     * @param memPtr Memory pointer.
     * @return {@code True} if owned by native platform.
     */
    public static boolean isExternal(long memPtr) {
        return isExternal(flags(memPtr));
    }

    /**
     * Check whether flags denote that this memory chunk is external.
     *
     * @param flags Flags.
     * @return {@code True} if owned by native platform.
     */
    public static boolean isExternal(int flags) {
        return (flags & FLAG_EXT) == FLAG_EXT;
    }

    /**
     * Check whether this memory chunk is pooled.
     *
     * @param memPtr Memory pointer.
     * @return {@code True} if pooled.
     */
    public static boolean isPooled(long memPtr) {
        return isPooled(flags(memPtr));
    }

    /**
     * Check whether flags denote pooled memory chunk.
     *
     * @param flags Flags.
     * @return {@code True} if pooled.
     */
    public static boolean isPooled(int flags) {
        return (flags & FLAG_POOLED) != 0;
    }

    /**
     * Check whether this memory chunk is pooled and acquired.
     *
     * @param memPtr Memory pointer.
     * @return {@code True} if pooled and acquired.
     */
    public static boolean isAcquired(long memPtr) {
        return isAcquired(flags(memPtr));
    }

    /**
     * Check whether flags denote pooled and acquired memory chunk.
     *
     * @param flags Flags.
     * @return {@code True} if acquired.
     */
    public static boolean isAcquired(int flags) {
        assert isPooled(flags);

        return (flags & FLAG_ACQUIRED) != 0;
    }

    /** --- UNPOOLED MEMORY MANAGEMENT. --- */

    /**
     * Allocate unpooled memory chunk.
     *
     * @param cap Minimum capacity.
     * @return New memory pointer.
     */
    public static long allocateUnpooled(int cap) {
        assert cap > 0;

        long memPtr = GridUnsafe.allocateMemory(MEM_HDR_LEN);
        long dataPtr = GridUnsafe.allocateMemory(cap);

        GridUnsafe.putLong(memPtr, dataPtr);              // Write address.
        GridUnsafe.putInt(memPtr + MEM_HDR_OFF_CAP, cap); // Write capacity.
        GridUnsafe.putInt(memPtr + MEM_HDR_OFF_LEN, 0);   // Write length.
        GridUnsafe.putInt(memPtr + MEM_HDR_OFF_FLAGS, 0); // Write flags.

        return memPtr;
    }

    /**
     * Reallocate unpooled memory chunk.
     *
     * @param memPtr Memory pointer.
     * @param cap Minimum capacity.
     */
    public static void reallocateUnpooled(long memPtr, int cap) {
        assert cap > 0;

        assert !isExternal(memPtr) : "Attempt to reallocate external memory chunk directly: " + memPtr;
        assert !isPooled(memPtr) : "Attempt to reallocate pooled memory chunk directly: " + memPtr;

        long dataPtr = data(memPtr);

        long newDataPtr = GridUnsafe.reallocateMemory(dataPtr, cap);

        if (dataPtr != newDataPtr)
            GridUnsafe.putLong(memPtr, newDataPtr); // Write new data address if needed.

        GridUnsafe.putInt(memPtr + MEM_HDR_OFF_CAP, cap); // Write new capacity.
    }

    /**
     * Release unpooled memory chunk.
     *
     * @param memPtr Memory pointer.
     */
    public static void releaseUnpooled(long memPtr) {
        assert !isExternal(memPtr) : "Attempt to release external memory chunk directly: " + memPtr;
        assert !isPooled(memPtr) : "Attempt to release pooled memory chunk directly: " + memPtr;

        GridUnsafe.freeMemory(data(memPtr));
        GridUnsafe.freeMemory(memPtr);
    }

    /** --- POOLED MEMORY MANAGEMENT. --- */

    /**
     * Allocate pool memory.
     *
     * @return Pool pointer.
     */
    public static long allocatePool() {
        long poolPtr = GridUnsafe.allocateMemory(POOL_HDR_LEN);

        GridUnsafe.zeroMemory(poolPtr, POOL_HDR_LEN);

        flags(poolPtr + POOL_HDR_OFF_MEM_1, FLAG_POOLED);
        flags(poolPtr + POOL_HDR_OFF_MEM_2, FLAG_POOLED);
        flags(poolPtr + POOL_HDR_OFF_MEM_3, FLAG_POOLED);

        return poolPtr;
    }

    /**
     * Release pool memory.
     *
     * @param poolPtr Pool pointer.
     */
    public static void releasePool(long poolPtr) {
        // Clean predefined memory chunks.
        long mem = GridUnsafe.getLong(poolPtr + POOL_HDR_OFF_MEM_1);

        if (mem != 0)
            GridUnsafe.freeMemory(mem);

        mem = GridUnsafe.getLong(poolPtr + POOL_HDR_OFF_MEM_2);

        if (mem != 0)
            GridUnsafe.freeMemory(mem);

        mem = GridUnsafe.getLong(poolPtr + POOL_HDR_OFF_MEM_3);

        if (mem != 0)
            GridUnsafe.freeMemory(mem);

        // Clean pool chunk.
        GridUnsafe.freeMemory(poolPtr);
    }

    /**
     * Allocate pooled memory chunk.
     *
     * @param poolPtr Pool pointer.
     * @param cap Capacity.
     * @return Cross-platform memory pointer or {@code 0} in case there are no free memory chunks in the pool.
     */
    public static long allocatePooled(long poolPtr, int cap) {
        long memPtr1 = poolPtr + POOL_HDR_OFF_MEM_1;

        if (isAcquired(memPtr1)) {
            long memPtr2 = poolPtr + POOL_HDR_OFF_MEM_2;

            if (isAcquired(memPtr2)) {
                long memPtr3 = poolPtr + POOL_HDR_OFF_MEM_3;

                if (isAcquired(memPtr3))
                    return 0L;
                else {
                    allocatePooled0(memPtr3, cap);

                    return memPtr3;
                }
            }
            else {
                allocatePooled0(memPtr2, cap);

                return memPtr2;
            }
        }
        else {
            allocatePooled0(memPtr1, cap);

            return memPtr1;
        }
    }

    /**
     * Internal pooled memory chunk allocation routine.
     *
     * @param memPtr Memory pointer.
     * @param cap Capacity.
     */
    private static void allocatePooled0(long memPtr, int cap) {
        assert !isExternal(memPtr);
        assert isPooled(memPtr);
        assert !isAcquired(memPtr);

        long data = GridUnsafe.getLong(memPtr);

        if (data == 0) {
            // First allocation of the chunk.
            data = GridUnsafe.allocateMemory(cap);

            GridUnsafe.putLong(memPtr, data);
            GridUnsafe.putInt(memPtr + MEM_HDR_OFF_CAP, cap);
        }
        else {
            // Ensure that we have enough capacity.
            int curCap = capacity(memPtr);

            if (cap > curCap) {
                data = GridUnsafe.reallocateMemory(data, cap);

                GridUnsafe.putLong(memPtr, data);
                GridUnsafe.putInt(memPtr + MEM_HDR_OFF_CAP, cap);
            }
        }

        flags(memPtr, FLAG_POOLED | FLAG_ACQUIRED);
    }

    /**
     * Reallocate pooled memory chunk.
     *
     * @param memPtr Memory pointer.
     * @param cap Minimum capacity.
     */
    public static void reallocatePooled(long memPtr, int cap) {
        assert !isExternal(memPtr);
        assert isPooled(memPtr);
        assert isAcquired(memPtr);

        long data = GridUnsafe.getLong(memPtr);

        assert data != 0;

        int curCap = capacity(memPtr);

        if (cap > curCap) {
            data = GridUnsafe.reallocateMemory(data, cap);

            GridUnsafe.putLong(memPtr, data);
            GridUnsafe.putInt(memPtr + MEM_HDR_OFF_CAP, cap);
        }
    }

    /**
     * Release pooled memory chunk.
     *
     * @param memPtr Memory pointer.
     */
    public static void releasePooled(long memPtr) {
        assert !isExternal(memPtr);
        assert isPooled(memPtr);
        assert isAcquired(memPtr);

        flags(memPtr, flags(memPtr) ^ FLAG_ACQUIRED);
    }

    /** --- UTILITY STUFF. --- */

    /**
     * Reallocate arbitrary memory chunk.
     *
     * @param memPtr Memory pointer.
     * @param cap Capacity.
     */
    public static void reallocate(long memPtr, int cap) {
        int flags = flags(memPtr);

        if (isPooled(flags))
            reallocatePooled(memPtr, cap);
        else {
            assert !isExternal(flags);

            reallocateUnpooled(memPtr, cap);
        }
    }

    /**
     * Constructor.
     */
    private PlatformMemoryUtils() {
        // No-op.
    }
}
