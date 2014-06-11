/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;
import org.gridgain.grid.util.*;
import sun.misc.*;

/**
 * Portable writer adapter.
 */
abstract class GridPortableWriterAdapter implements GridPortableWriter {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    private static final int INIT_SIZE = 4 * 1024;

    /** */
    private byte[] arr;

    /** */
    private int off;

    /** */
    protected GridPortableWriterAdapter() {
        arr = new byte[INIT_SIZE];
    }

    /**
     * @param bytes Number of bytes that are going to be written.
     */
    protected void requestFreeSize(int bytes) {
        int newSize = off + bytes;

        if (off + bytes > arr.length) {
            byte[] arr0 = new byte[newSize << 1];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF, arr0, BYTE_ARR_OFF, off);

            arr = arr0;
        }
    }

    /**
     * @return Array.
     */
    public byte[] array() {
        byte[] arr0 = new byte[off];

        UNSAFE.copyMemory(arr, BYTE_ARR_OFF, arr0, BYTE_ARR_OFF, off);

        return arr0;
    }
}
