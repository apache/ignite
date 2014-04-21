/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap.unsafe;

import org.gridgain.grid.util.offheap.unsafe.reproducing.*;

import java.util.*;

/**
 * Off-heap map based on {@code Unsafe} implementation.
 */
public class GridUnsafeMap0 {
    /** Total memory. */
    private final GridUnsafeMemory mem;

    @SuppressWarnings("unchecked")
    public GridUnsafeMap0() throws Exception {
        mem = new GridUnsafeMemory(10000);
    }

    private Deque<Long> q = new ArrayDeque<>();

    public void put0(byte[] keyBytes, byte[] valBytes) {
        long ptr = mem.allocate(28 + keyBytes.length + valBytes.length);

        mem.writeInt(ptr + 4, keyBytes.length);
        mem.writeInt(ptr + 8, valBytes.length);

        mem.writeBytes(ptr + 28, keyBytes);
        mem.writeBytes(ptr + 28 + keyBytes.length, valBytes);

        q.addLast(ptr);

        if (q.size() > 100) {
            long ptr0 = q.pollFirst();

            byte[] valBytes0 = UnsafeWrapper.faultyMethod(ptr0);

            if (valBytes0[0] != 114)
                System.out.println("PROBLEM: " + Arrays.toString(valBytes0));

            int keySize = mem.readInt(ptr0 + 4);
            int valSize = mem.readInt(ptr0 + 8);

            mem.release(ptr0, 28 + keySize + valSize);
        }
    }
}
