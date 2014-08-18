/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller.optimized;

import org.gridgain.grid.util.*;
import sun.misc.*;

/**
 */
public class GridDirectByteBuffer {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long ARR_OFF = (long)UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    static final int UNSAFE_COPY_THRESHOLD = 1024 * 1024;

    /** */
    private final long addr;

    /** */
    private final int size;

    /**
     * @param addr Address.
     */
    public GridDirectByteBuffer(long addr, int size) {
        this.addr = addr;
        this.size = size;
    }

    public int size() {
        return size;
    }

    /**
     * @param pos Position.
     * @return Byte value.
     */
    public byte get(int pos) {
        return UNSAFE.getByte(address(pos));
    }

    /**
     * @param pos Position.
     * @return Byte value.
     */
    public int getInt(int pos) {
        return UNSAFE.getInt(address(pos));
    }

    /**
     * @param srcPos
     * @param dst
     * @param dstPos
     * @param len
     */
    public void copyTo(int srcPos, byte[] dst, int dstPos, int len) {
        copyToArray(address(srcPos), dst, ARR_OFF, dstPos << 0, len << 0);
    }

    /**
     * @param srcAddr
     * @param dst
     * @param dstBaseOffset
     * @param dstPos
     * @param len
     */
    static void copyToArray(long srcAddr, Object dst, long dstBaseOffset, long dstPos, long len) {
        long offset = dstBaseOffset + dstPos;

        while (len > 0) {
            long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
            UNSAFE.copyMemory(null, srcAddr, dst, offset, size);
            len -= size;
            srcAddr += size;
            offset += size;
        }
    }

    /**
     * @param pos Position.
     * @return Address.
     */
    private long address(int pos) {
        return addr + (pos << 0);
    }
}
