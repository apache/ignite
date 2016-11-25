package org.apache.ignite.internal.processors.hadoop.shuffle.mem.offheap;

import org.apache.ignite.internal.processors.hadoop.shuffle.mem.Page;

/**
 * Page.
 */
public class OffheapPage implements Page {
    /** Pointer. */
    private final long ptr;

    /** Size. */
    private final long size;

    /**
     * Constructor.
     *
     * @param ptr Pointer.
     * @param size Size.
     */
    public OffheapPage(long ptr, long size) {
        this.ptr = ptr;
        this.size = size;
    }

    /**
     * @return Pointer.
     */
    public long ptr() {
        return ptr;
    }

    /**
     * @return Page size.
     */
    @Override public long size() {
        return size;
    }
}
