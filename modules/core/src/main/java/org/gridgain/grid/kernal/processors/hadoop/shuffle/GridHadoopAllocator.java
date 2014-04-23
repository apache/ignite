/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.util.*;
import org.gridgain.grid.util.offheap.unsafe.*;

/**
 * Memory allocator.
 */
public class GridHadoopAllocator implements AutoCloseable {
    /** */
    public static final int PAGE_SIZE = GridUnsafe.unsafe().pageSize();

    /** */
    private int pageSize;

    /** */
    private final GridLongList pagePtrs = new GridLongList();

    /** */
    private final GridUnsafeMemory mem;

    /**
     * @param mem Memory.
     * @param pageSize Page size.
     */
    public GridHadoopAllocator(GridUnsafeMemory mem, int pageSize) {
        this.mem = mem;
        this.pageSize = pageSize;
    }

    /**
     * @param mem Memory.
     */
    public GridHadoopAllocator(GridUnsafeMemory mem) {
        this(mem, PAGE_SIZE);
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    public void pageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public long nextPage() {
        return nextPage(pageSize);
    }

    public long nextPage(int pageSize) {
        if (!mem.tryReserve(pageSize))
            return -1;

        long ptr = mem.allocate(pageSize, false, true);

        synchronized (pagePtrs) {
            pagePtrs.add(ptr);
            pagePtrs.add(pageSize);
        }

        return ptr;
    }

    public long allocatedSize() {
        long size = 0;

        synchronized (pagePtrs) {
            for (int i = 1; i < pagePtrs.size(); i += 2)
                size += pagePtrs.get(i);
        }

        return size;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        synchronized (pagePtrs) {
            for (int i = 0; i < pagePtrs.size(); i += 2)
                mem.release(pagePtrs.get(i), pagePtrs.get(i + 1));

            pagePtrs.truncate(0, true);
        }
    }
}
