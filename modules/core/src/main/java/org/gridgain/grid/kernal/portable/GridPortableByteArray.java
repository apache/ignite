/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.util.*;
import sun.misc.*;

/**
 * Byte array wrapper.
 */
class GridPortableByteArray {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    private byte[] arr;

    /** */
    private int size;

    /**
     * @param cap Initial capacity.
     */
    GridPortableByteArray(int cap) {
        arr = new byte[cap];
    }

    /**
     * @param bytes Number of bytes that are going to be written.
     * @return Offset before write.
     */
    int requestFreeSize(int bytes) {
        int size0 = size;

        size += bytes;

        if (size > arr.length) {
            byte[] arr0 = new byte[size << 1];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF, arr0, BYTE_ARR_OFF, size0);

            arr = arr0;
        }

        return size0;
    }

    /**
     * @return Array.
     */
    byte[] array() {
        return arr;
    }

    /**
     * @return Size.
     */
    int size() {
        return size;
    }

    /**
     * @return Array copy.
     */
    byte[] entireArray() {
        if (arr.length == size)
            return arr;
        else {
            byte[] arr0 = new byte[size];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF, arr0, BYTE_ARR_OFF, size);

            return arr0;
        }
    }
}
